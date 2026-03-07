import mandrill, secrets, smtplib, time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from ..utils.redis_compat import AsyncRedisClient

#-----------------------------------------------------------------------------

class AbstractEmailCodeValidator:
    async def send(self, to_email: str, expires_in: int = 0, service: str = "") -> str | None: ...
    async def verify(self, to_email: str, code: str, service: str = "") -> str | None: ...

#-----------------------------------------------------------------------------

class DummyEmailCodeValidator(AbstractEmailCodeValidator):
    def __init__(
        self,
        predefined_codes: dict[str, str] | None = None
    ):
        self._predefined_codes = predefined_codes if isinstance(predefined_codes, dict) else {}

    async def send(self, to_email: str, expires_in: int = 0, service: str = "") -> str | None:
        return "No SMTP server configured."
    
    async def verify(self, to_email: str, code: str, service: str = "") -> str | None:
        if self._predefined_codes:
            if to_email in self._predefined_codes and self._predefined_codes[to_email] == code:
                return None
        
        return "No SMTP server configured."

#-----------------------------------------------------------------------------

class MandrillEmailValidator(AbstractEmailCodeValidator):
    def __init__(
            self,
            apiKey          : str,
            template        : str,
            from_email      : str,
            from_name       : str,
            sending_interval: int = 60,
            expires_in      : int = 10*60,
            predefined_codes: dict[str, str] | None = None,
            redis           : AsyncRedisClient | None = None
        ):

        self._mandrill_client = None
        if apiKey and isinstance(apiKey, str):
            self._mandrill_client = mandrill.Mandrill(apiKey)

        self._template          = template
        self._from_email        = from_email
        self._from_name         = from_name

        self._sending_interval  = sending_interval
        self._expires_in        = expires_in

        self._predefined_codes = predefined_codes if predefined_codes else {}

        #-------------------------------------------------
        # Limitation on code sending.

        # Use remote memory when redis connection is available.
        self._redis = redis

        if self._redis:
            self._code_keyprefix    = "mirobody:email:code:"
            self._limit_keyprefix   = "mirobody:email:limit:"

        # Use local memory when no redis connection is available.
        self._codes = {}

    #-----------------------------------------------------

    async def send(self, to_email: str, expires_in: int = 0, service: str = "") -> str | None:
        # Check the input.
        if not to_email or not isinstance(to_email, str):
            return "Invalid email address."
        
        lower_email = to_email.strip().lower()
        if not lower_email or "@" not in lower_email:
            return "Invalid email address."
        
        # Return straightly if it is a predefined email address.
        if hasattr(self, '_predefined_codes') and self._predefined_codes and lower_email in self._predefined_codes:
            return None

        if not self._mandrill_client:
            return "Invalid email client."
        
        #-------------------------------------------------

        code = str(secrets.randbelow(1000000)).zfill(6)
        
        formatted_code = "".join([f"<span>{digit}</span>" for digit in code])
        
        result = self._mandrill_client.messages.send_template(
            template_name   = self._template,
            template_content= [
                {
                    "name"      : "CODE",
                    "content"   : formatted_code
                }
            ],
            message         = {
                "subject"   : "Your Verification Code",
                "from_email": self._from_email,
                "from_name" : self._from_name,
                "to": [
                    {
                        "email" : lower_email,
                        "type"  : "to"
                    }
                ],
                "merge_vars": [
                    {
                        "rcpt": lower_email,
                        "vars": [
                            {
                                "name"      : "CODE",
                                "content"   : formatted_code
                            }
                        ],
                    }
                ]
            },
            send_async      = False
        )
        
        #-------------------------------------------------

        if service and isinstance(service, str):
            lower_email_with_service = lower_email + ":" + service
        else:
            lower_email_with_service = lower_email

        actual_expires_in = self._expires_in
        if isinstance(expires_in, int) and expires_in > 0:
            actual_expires_in = expires_in

        if result and len(result) > 0 and result[0].get("status") in ["sent", "queued"]:
            if self._redis:
                # Used to verify the code sending via email.
                key = self._code_keyprefix + lower_email_with_service
                await self._redis.set(key, code)
                await self._redis.expire(key, actual_expires_in)

                # Used to avoid sending duplicately.
                key = self._limit_keyprefix + lower_email_with_service
                await self._redis.set(key, code)
                await self._redis.expire(key, self._sending_interval)

            else:
                now = time.time()

                # Record this code.
                self._codes[lower_email_with_service] = {
                    "value"     : code,
                    "cold_down" : now + self._sending_interval,
                    "expires_at": now + actual_expires_in
                }

                # Remove expired codes at the same time.
                to_be_deleted = []

                for key in self._codes:
                    if self._codes[key]["expires_at"] < now:
                        to_be_deleted.append(key)

                for key in to_be_deleted:
                    del self._codes[key]
                    
            return None
        
        else:
            return f"Failed to send email to {lower_email}: {result}"

    #-----------------------------------------------------

    async def verify(self, to_email: str, code: str, service: str = "") -> str | None:
        # Check the input.
        if not to_email or not isinstance(to_email, str):
            return "Invalid email address."
        
        lower_email = to_email.strip().lower()
        if not lower_email or "@" not in lower_email:
            return "Invalid email address"
        
        # Return straightly if it is a predefined email address.
        if hasattr(self, '_predefined_codes') and self._predefined_codes and lower_email in self._predefined_codes:
            return None if self._predefined_codes[lower_email] == code else "Invalid code"
        
        if not code:
            return "Empty code."
        
        for digit in code:
            if digit < '0' or digit > '9':
                return "Invalid code."
        
        #-------------------------------------------------

        if service and isinstance(service, str):
            lower_email_with_service = lower_email + ":" + service
        else:
            lower_email_with_service = lower_email
        
        if self._redis:
            resp = await self._redis.get(self._code_keyprefix + lower_email_with_service)
            if isinstance(resp, str) and resp == code:
                # Everything is fine.
                return None

        else:
            if  lower_email_with_service in self._codes:
                # Remove it if it expires.
                if self._codes[lower_email_with_service]["expires_at"] < time.time():
                    del self._codes[lower_email_with_service]

                    return "Code expired."

                # Check its value.
                if self._codes[lower_email_with_service]["value"] == code:
                    del self._codes[lower_email_with_service]

                    # Everything is fine.
                    return None

        return "Invalid code."

#-----------------------------------------------------------------------------

class SMTPEmailValidator(AbstractEmailCodeValidator):
    """Email validator using direct SMTP connection (e.g., Aliyun DirectMail)."""
    
    def __init__(
            self,
            smtp_host       : str,
            smtp_port       : int,
            smtp_user       : str,
            smtp_pass       : str,
            from_email      : str,
            from_name       : str,
            sending_interval: int = 60,
            expires_in      : int = 10*60,
            predefined_codes: dict[str, str] | None = None,
            redis           : AsyncRedisClient | None = None
        ):

        self._smtp_host     = smtp_host
        self._smtp_port     = smtp_port
        self._smtp_user     = smtp_user
        self._smtp_pass     = smtp_pass
        self._from_email    = from_email
        self._from_name     = from_name

        self._sending_interval  = sending_interval
        self._expires_in        = expires_in

        self._predefined_codes = predefined_codes if predefined_codes else {}

        # Use remote memory when redis connection is available.
        self._redis = redis

        if self._redis:
            self._code_keyprefix    = "mirobody:email:code:"
            self._limit_keyprefix   = "mirobody:email:limit:"

        # Use local memory when no redis connection is available.
        self._codes = {}

    #-----------------------------------------------------

    def _build_html_email(self, code: str) -> str:
        """Build HTML email content with verification code."""
        formatted_code = " ".join(code)
        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        .code-container {{ text-align: center; margin: 20px 0; }}
        .code {{ font-size: 32px; font-weight: bold; letter-spacing: 8px; color: #333; }}
    </style>
</head>
<body>
    <div style="max-width: 600px; margin: 0 auto; padding: 20px; font-family: Arial, sans-serif;">
        <h2 style="color: #333;">Your Verification Code</h2>
        <p>Please use the following code to complete your verification:</p>
        <div class="code-container">
            <span class="code">{formatted_code}</span>
        </div>
        <p style="color: #666; font-size: 14px;">This code will expire in 10 minutes.</p>
        <p style="color: #999; font-size: 12px;">If you did not request this code, please ignore this email.</p>
    </div>
</body>
</html>
"""

    #-----------------------------------------------------

    async def send(self, to_email: str, expires_in: int = 0, service: str = "") -> str | None:
        # Check the input.
        if not to_email or not isinstance(to_email, str):
            return "Invalid email address."
        
        lower_email = to_email.strip().lower()
        if not lower_email or "@" not in lower_email:
            return "Invalid email address."
        
        # Return straightly if it is a predefined email address.
        if hasattr(self, '_predefined_codes') and self._predefined_codes and lower_email in self._predefined_codes:
            return None

        if not self._smtp_host or not self._smtp_user or not self._smtp_pass:
            return "Invalid SMTP configuration."
        
        #-------------------------------------------------

        code = str(secrets.randbelow(1000000)).zfill(6)
        
        # Build the email
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Your Verification Code"
        msg['From'] = f"{self._from_name} <{self._from_email}>"
        msg['To'] = lower_email

        html_content = self._build_html_email(code)
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))

        #-------------------------------------------------
        # Send via SMTP

        try:
            if self._smtp_port == 465:
                # SSL connection
                with smtplib.SMTP_SSL(self._smtp_host, self._smtp_port) as server:
                    server.login(self._smtp_user, self._smtp_pass)
                    server.sendmail(self._from_email, [lower_email], msg.as_string())
            else:
                # STARTTLS connection (port 587 or others)
                with smtplib.SMTP(self._smtp_host, self._smtp_port) as server:
                    server.starttls()
                    server.login(self._smtp_user, self._smtp_pass)
                    server.sendmail(self._from_email, [lower_email], msg.as_string())

        except Exception as e:
            return f"Failed to send email: {str(e)}"
        
        #-------------------------------------------------

        if service and isinstance(service, str):
            lower_email_with_service = lower_email + ":" + service
        else:
            lower_email_with_service = lower_email

        actual_expires_in = self._expires_in
        if isinstance(expires_in, int) and expires_in > 0:
            actual_expires_in = expires_in

        # Store the code
        if self._redis:
            # Used to verify the code sending via email.
            key = self._code_keyprefix + lower_email_with_service
            await self._redis.set(key, code)
            await self._redis.expire(key, actual_expires_in)

            # Used to avoid sending duplicately.
            key = self._limit_keyprefix + lower_email_with_service
            await self._redis.set(key, code)
            await self._redis.expire(key, self._sending_interval)

        else:
            now = time.time()

            # Record this code.
            self._codes[lower_email_with_service] = {
                "value"     : code,
                "cold_down" : now + self._sending_interval,
                "expires_at": now + actual_expires_in
            }

            # Remove expired codes at the same time.
            to_be_deleted = []

            for key in self._codes:
                if self._codes[key]["expires_at"] < now:
                    to_be_deleted.append(key)

            for key in to_be_deleted:
                del self._codes[key]
                    
        return None

    #-----------------------------------------------------

    async def verify(self, to_email: str, code: str, service: str = "") -> str | None:
        # Check the input.
        if not to_email or not isinstance(to_email, str):
            return "Invalid email address."
        
        lower_email = to_email.strip().lower()
        if not lower_email or "@" not in lower_email:
            return "Invalid email address"
        
        # Return straightly if it is a predefined email address.
        if hasattr(self, '_predefined_codes') and self._predefined_codes and lower_email in self._predefined_codes:
            return None if self._predefined_codes[lower_email] == code else "Invalid code"
        
        if not code:
            return "Empty code."
        
        for digit in code:
            if digit < '0' or digit > '9':
                return "Invalid code."
        
        #-------------------------------------------------

        if service and isinstance(service, str):
            lower_email_with_service = lower_email + ":" + service
        else:
            lower_email_with_service = lower_email
        
        if self._redis:
            resp = await self._redis.get(self._code_keyprefix + lower_email_with_service)
            if isinstance(resp, str) and resp == code:
                # Everything is fine.
                return None

        else:
            if  lower_email_with_service in self._codes:
                # Remove it if it expires.
                if self._codes[lower_email_with_service]["expires_at"] < time.time():
                    del self._codes[lower_email_with_service]

                    return "Code expired."

                # Check its value.
                if self._codes[lower_email_with_service]["value"] == code:
                    del self._codes[lower_email_with_service]

                    # Everything is fine.
                    return None

        return "Invalid code."

#-----------------------------------------------------------------------------

def create_email_validator(
    # SMTP configuration (preferred)
    smtp_host       : str = "",
    smtp_port       : int = 0,
    smtp_user       : str = "",
    smtp_pass       : str = "",
    
    # Mandrill configuration (fallback)
    mandrill_api_key: str = "",
    
    # Common configuration
    from_email      : str = "",
    from_name       : str = "",
    template        : str = "",
    
    # Other options
    predefined_codes: dict[str, str] | None = None,
    redis           : AsyncRedisClient | None = None
) -> AbstractEmailCodeValidator:
    """
    Factory function to create the appropriate email validator based on configuration.
    
    Priority:
    1. Mandrill via SMTP config (if smtp_host contains 'mandrillapp.com', use smtp_pass as API key)
    2. SMTP configuration (if smtp_host, smtp_user, smtp_pass are provided)
    3. Mandrill configuration (if mandrill_api_key is provided)
    4. DummyEmailCodeValidator (fallback)
    """
    
    # Priority 1: Auto-detect Mandrill from smtp_host
    if smtp_host and "mandrillapp.com" in smtp_host.lower() and smtp_pass and from_email and template:
        return MandrillEmailValidator(
            apiKey          = smtp_pass,  # Use smtp_pass as Mandrill API key
            template        = template,
            from_email      = from_email,
            from_name       = from_name if from_name else "Theta Wellness",
            predefined_codes= predefined_codes,
            redis           = redis
        )
    
    # Priority 2: SMTP
    if smtp_host and smtp_user and smtp_pass and from_email:
        return SMTPEmailValidator(
            smtp_host       = smtp_host,
            smtp_port       = smtp_port if smtp_port > 0 else 465,
            smtp_user       = smtp_user,
            smtp_pass       = smtp_pass,
            from_email      = from_email,
            from_name       = from_name if from_name else "Theta Wellness",
            predefined_codes= predefined_codes,
            redis           = redis
        )
    
    # Priority 3: Mandrill (legacy, with explicit mandrill_api_key)
    if mandrill_api_key and from_email and template:
        return MandrillEmailValidator(
            apiKey          = mandrill_api_key,
            template        = template,
            from_email      = from_email,
            from_name       = from_name if from_name else "Theta Wellness",
            predefined_codes= predefined_codes,
            redis           = redis
        )
    
    # Fallback: Dummy
    return DummyEmailCodeValidator(predefined_codes=predefined_codes)

#-----------------------------------------------------------------------------

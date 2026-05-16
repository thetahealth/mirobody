import aiohttp, logging

#-----------------------------------------------------------------------------

class WeChatOpenValidator:
    """
    WeChat Open Platform (open.weixin.qq.com) - Website App QR login.
    Exchanges the OAuth `code` returned by qrconnect for openid.
    The response also carries `unionid` and a refresh_token; we currently
    only persist openid (single-app deployment, see user_service.py).
    Docs: https://developers.weixin.qq.com/doc/oplatform/Website_App/WeChat_Login/Wechat_Login.html
    """

    ACCESS_TOKEN_URL = "https://api.weixin.qq.com/sns/oauth2/access_token"
    USER_INFO_URL    = "https://api.weixin.qq.com/sns/userinfo"

    #-----------------------------------------------------

    def __init__(self, appid: str, secret: str):
        self._appid  = appid
        self._secret = secret

    #-----------------------------------------------------

    async def exchange_code(self, code: str) -> tuple[dict | None, str | None]:
        if not code:
            return None, "Empty WeChat authorization code."

        params = {
            "appid"     : self._appid,
            "secret"    : self._secret,
            "code"      : code,
            "grant_type": "authorization_code",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.ACCESS_TOKEN_URL, params=params) as resp:
                    body = await resp.json(content_type=None)
        except Exception as e:
            logging.error(f"WeChat exchange_code request failed: {e}")
            return None, f"WeChat request failed: {e}"

        if not isinstance(body, dict):
            return None, "Invalid WeChat response."

        if body.get("errcode"):
            errcode = body.get("errcode")
            errmsg  = body.get("errmsg", "")
            logging.warning(f"WeChat exchange_code error {errcode}: {errmsg}")
            return None, f"WeChat error {errcode}: {errmsg}"

        if not body.get("openid"):
            return None, "WeChat response missing openid."

        return body, None

    #-----------------------------------------------------

    async def fetch_user_info(
        self,
        access_token   : str,
        openid         : str,
        lang           : str = "zh_CN",
    ) -> tuple[dict | None, str | None]:
        """
        Fetch nickname / avatar / city via /sns/userinfo.

        Best-effort — failures (network, errcode) return (None, err) so the
        caller can fall through to placeholder data without aborting login.
        Returned shape: {"openid": "...", "nickname": "...",
                         "headimgurl": "...", "unionid": "...", ...}
        """
        if not access_token or not openid:
            return None, "Empty access_token or openid."

        params = {
            "access_token": access_token,
            "openid"      : openid,
            "lang"        : lang,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.USER_INFO_URL, params=params) as resp:
                    body = await resp.json(content_type=None)
        except Exception as e:
            logging.warning(f"WeChat fetch_user_info request failed: {e}")
            return None, f"WeChat request failed: {e}"

        if not isinstance(body, dict):
            return None, "Invalid WeChat response."

        if body.get("errcode"):
            errcode = body.get("errcode")
            errmsg  = body.get("errmsg", "")
            logging.warning(f"WeChat userinfo error {errcode}: {errmsg}")
            return None, f"WeChat error {errcode}: {errmsg}"

        return body, None

#-----------------------------------------------------------------------------

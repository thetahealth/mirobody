"""
File Processing Module

Provides various file extraction and processing functions.
Automatically selects appropriate vision model provider based on configured API keys.
"""

import base64
import logging
import pathlib
import time
import io
from typing import Optional, Tuple, Dict, Any, List

import pypdfium2 as pdfium
from google.genai import types
from PIL import Image
from openai import AsyncOpenAI
from volcenginesdkarkruntime import AsyncArk

from mirobody.utils.config import safe_read_cfg

from .clients import client_manager
from .config import AIConfig


class VisionProviderConfig:
    """
    Vision Provider Configuration
    
    Automatically selects appropriate vision model provider based on available API keys.
    Priority order: gemini > openrouter > doubao
    """
    
    # Vision provider configuration list (sorted by priority)
    VISION_PROVIDERS: List[Dict[str, Any]] = [
        {
            "name": "gemini",
            "api_key_env": "GOOGLE_API_KEY",
            "default_model": "gemini-3-flash-preview",
            "description": "Google Gemini (Direct API)",
        },
        {
            "name": "openrouter",
            "api_key_env": "OPENROUTER_API_KEY",
            "default_model": "google/gemini-3-flash-preview",
            "description": "OpenRouter (OpenAI Compatible)",
        },
        {
            "name": "doubao",
            "api_key_env": "VOLCENGINE_API_KEY",
            "default_model": "doubao-seed-1-6-vision-250815",
            "description": "Doubao/Volcengine Vision",
        },
    ]
    
    @classmethod
    def get_available_provider(cls) -> Optional[Dict[str, Any]]:
        """
        Get the first available vision provider (based on configured API keys)
        
        Returns:
            Provider configuration dict, or None if no provider is available
        """
        for provider in cls.VISION_PROVIDERS:
            api_key = safe_read_cfg(provider["api_key_env"])
            if api_key:
                logging.info(f"🔍 Vision provider selected: {provider['name']} ({provider['description']})")
                return provider
        return None
    
    @classmethod
    def get_provider_by_name(cls, name: str) -> Optional[Dict[str, Any]]:
        """
        Get provider configuration by name
        
        Args:
            name: Provider name (gemini/openrouter/doubao)
            
        Returns:
            Provider configuration dict
        """
        for provider in cls.VISION_PROVIDERS:
            if provider["name"] == name:
                return provider
        return None
    
    @classmethod
    def list_available_providers(cls) -> List[str]:
        """
        List all available providers with configured API keys
        
        Returns:
            List of available provider names
        """
        available = []
        for provider in cls.VISION_PROVIDERS:
            api_key = safe_read_cfg(provider["api_key_env"])
            if api_key:
                available.append(provider["name"])
        return available
    
    @classmethod
    def get_provider_status(cls) -> Dict[str, bool]:
        """
        Get configuration status of all providers
        
        Returns:
            Mapping of provider names to availability status
        """
        return {
            provider["name"]: bool(safe_read_cfg(provider["api_key_env"]))
            for provider in cls.VISION_PROVIDERS
        }


def clean_json_response(response: str) -> str:
    """
    Clean LLM response by removing markdown code block markers.
    
    Args:
        response: Raw LLM response
        
    Returns:
        Cleaned JSON string
    """
    if not response:
        return response
        
    response = response.strip()
    
    # Remove markdown code block markers
    if response.startswith('```json'):
        response = response[7:]
    elif response.startswith('```'):
        response = response[3:]
        
    if response.endswith('```'):
        response = response[:-3]
        
    return response.strip()


class FileProcessor:
    """Base class for file processors"""
    
    @staticmethod
    def optimize_image_for_llm(
        image_data: bytes, 
        max_dimension: int = 2048,
        quality: int = 85,
        format: str = "JPEG"
    ) -> Tuple[bytes, dict]:
        """
        Optimize image for LLM processing by reducing resolution and applying compression
        
        Args:
            image_data: Original image data in bytes
            max_dimension: Maximum dimension (width or height) for the image
            quality: JPEG quality (1-100, higher is better quality but larger size)
            format: Output format (JPEG recommended for best compression)
            
        Returns:
            Tuple[bytes, dict]: (Optimized image data, optimization stats)
        """
        try:
            start_time = time.time()
            original_size = len(image_data)
            
            # Open image from bytes
            img = Image.open(io.BytesIO(image_data))
            original_width, original_height = img.size
            
            # Convert RGBA to RGB if necessary (for JPEG compatibility)
            if img.mode in ('RGBA', 'LA', 'P'):
                # Create a white background
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                img = background
            elif img.mode not in ('RGB', 'L'):
                img = img.convert('RGB')
            
            # Calculate new dimensions if image is too large
            width, height = img.size
            if width > max_dimension or height > max_dimension:
                # Calculate scaling factor
                scale = min(max_dimension / width, max_dimension / height)
                new_width = int(width * scale)
                new_height = int(height * scale)
                
                # Resize image using high-quality resampling
                img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                logging.info(f"📐 Image resized from {original_width}x{original_height} to {new_width}x{new_height}")
            
            # Save optimized image to bytes
            output = io.BytesIO()
            if format == "JPEG":
                # Use optimize flag for better compression
                img.save(output, format=format, quality=quality, optimize=True, progressive=True)
            else:
                img.save(output, format=format, quality=quality, optimize=True)
            
            optimized_data = output.getvalue()
            optimized_size = len(optimized_data)
            
            # Calculate statistics
            compression_ratio = (1 - optimized_size / original_size) * 100
            processing_time = time.time() - start_time
            
            stats = {
                "original_size": original_size,
                "optimized_size": optimized_size,
                "compression_ratio": compression_ratio,
                "original_dimensions": (original_width, original_height),
                "optimized_dimensions": img.size,
                "processing_time": processing_time
            }
            
            logging.info(f"✅ Image optimized: {original_size/1024:.1f}KB → {optimized_size/1024:.1f}KB "
                      f"({compression_ratio:.1f}% reduction), took {processing_time:.2f}s")
            
            return optimized_data, stats
            
        except Exception as e:
            logging.warning(f"Image optimization failed, using original: {str(e)}")
            # Return original data if optimization fails
            return image_data, {"error": str(e), "original_size": len(image_data)}

    @staticmethod
    async def gemini_file_extract(
        local_file_path: str,
        mime_type: str,
        prompt: str,
        config: Optional[types.GenerateContentConfig] = None,
        model: str = "gemini-3-flash-preview",
    ) -> str:
        """
        Extract file content using Gemini model

        Args:
            local_file_path: Local file path
            mime_type: File MIME type
            prompt: Extraction prompt
            config: Generation configuration
            model: Model name

        Returns:
            Extracted file content
        """
        try:
            filepath = pathlib.Path(local_file_path)
            if not filepath.exists():
                logging.error(f"File not found: {local_file_path}")
                return ""

            client = client_manager.get_async_gemini_client()

            if config is None:
                config = types.GenerateContentConfig(temperature=0.1)

            # Read file data
            file_data = filepath.read_bytes()
            
            # Optimize image files before sending to API
            if mime_type and mime_type.startswith('image/'):
                optimized_data, stats = FileProcessor.optimize_image_for_llm(
                    file_data,
                    max_dimension=1536,  # Suitable resolution for medical reports
                    quality=85  # Maintain high quality for text clarity
                )
                logging.info(f"Gemini: Image optimized for processing, {stats}")
                file_data = optimized_data
            
            response = await client.models.generate_content(
                model=model,
                contents=[
                    types.Part.from_bytes(
                        data=file_data,
                        mime_type=mime_type,
                    ),
                    prompt,
                ],
                config=config,
            )

            # Check if response is valid
            if not response:
                logging.error("Gemini API returned empty response")
                return ""

            # Check if response has text content
            if not hasattr(response, "text") or response.text is None:
                logging.error("Gemini API response has no text content")
                return ""

            # Check for candidates and content safety
            if hasattr(response, "candidates") and response.candidates:
                candidate = response.candidates[0]
                if hasattr(candidate, "finish_reason"):
                    if candidate.finish_reason in ["SAFETY", "BLOCKED"]:
                        logging.warning(f"Content blocked by safety filter: {candidate.finish_reason}")
                        return ""
                    elif candidate.finish_reason == "OTHER":
                        logging.warning("Content generation stopped for unknown reason")
                        return ""

            return response.text or ""

        except Exception as e:
            error_msg = str(e)
            logging.error(f"Gemini file processing failed: {type(e).__name__}: {error_msg}", stack_info=True)
            # Check for specific error types and raise with appropriate message
            if "User location is not supported" in error_msg:
                raise ValueError("Gemini API error: User location is not supported for the API use. Please check your API region settings.")
            elif "400" in error_msg or "FAILED_PRECONDITION" in error_msg:
                raise ValueError(f"Gemini API configuration error: {error_msg}")
            elif "401" in error_msg or "403" in error_msg:
                raise ValueError(f"Gemini API authentication error: {error_msg}")
            elif "429" in error_msg:
                raise ValueError(f"Gemini API rate limit exceeded: {error_msg}")
            else:
                raise ValueError(f"Gemini API failed: {error_msg}") from e

    @staticmethod
    async def gemini_multi_file_extract(
        files: List[Dict[str, str]],
        prompt: str,
        config: Optional[types.GenerateContentConfig] = None,
        model: str = "gemini-3-flash-preview",
    ) -> str:
        """
        Use Gemini model to process multiple files at once
        
        Args:
            files: List of file dictionaries, each containing:
                   - 'path': local file path
                   - 'mime_type': file MIME type
            prompt: Prompt text
            config: Generation configuration
            model: Model name
            
        Returns:
            Extracted content from files
        """
        try:
            # Validate files list is not empty
            if not files:
                logging.error("Files list is empty")
                raise ValueError("Files list cannot be empty")
            
            # Validate each file has required keys
            for idx, file in enumerate(files):
                if not isinstance(file, dict):
                    logging.error(f"File at index {idx} is not a dictionary")
                    raise ValueError(f"File at index {idx} must be a dictionary")
                if "path" not in file:
                    logging.error(f"File at index {idx} is missing 'path' key")
                    raise ValueError(f"File at index {idx} is missing required 'path' key")
                if "mime_type" not in file:
                    logging.error(f"File at index {idx} is missing 'mime_type' key")
                    raise ValueError(f"File at index {idx} is missing required 'mime_type' key")
            
            # Check if all files exist
            for file in files:
                filepath = pathlib.Path(file["path"])
                if not filepath.exists():
                    logging.error(f"File does not exist: {file['path']}")
                    raise ValueError(f"File does not exist: {file['path']}")
            
            # Get Gemini client
            client = client_manager.get_async_gemini_client()
            
            # Set default config if not provided
            if config is None:
                config = types.GenerateContentConfig(temperature=0.1)
            
            # Build contents list with all files
            contents = []
            
            # Add all file parts
            for file in files:
                filepath = pathlib.Path(file["path"])
                file_data = filepath.read_bytes()
                
                # Optimize image files before sending to API
                if file["mime_type"] and file["mime_type"].startswith('image/'):
                    optimized_data, stats = FileProcessor.optimize_image_for_llm(
                        file_data,
                        max_dimension=1536,  # Suitable resolution for medical reports
                        quality=85  # Maintain high quality for text clarity
                    )
                    logging.info(f"Gemini: Image {file['path']} optimized for processing, {stats}")
                    file_data = optimized_data
                
                file_part = types.Part.from_bytes(
                    data=file_data,
                    mime_type=file["mime_type"],
                )
                contents.append(file_part)
            
            # Add prompt at the end
            contents.append(prompt)
            
            # Call Gemini API
            response = await client.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
            
            # Check if response is valid
            if not response:
                logging.error("Gemini API returned empty response")
                return ""
            
            # Check if response has text content
            if not hasattr(response, "text") or response.text is None:
                logging.error("Gemini API response has no text content")
                return ""
            
            # Check for candidates and content safety
            if hasattr(response, "candidates") and response.candidates:
                candidate = response.candidates[0]
                if hasattr(candidate, "finish_reason"):
                    if candidate.finish_reason in ["SAFETY", "BLOCKED"]:
                        logging.warning(f"Content blocked by safety filter: {candidate.finish_reason}")
                        return ""
                    elif candidate.finish_reason == "OTHER":
                        logging.warning("Content generation stopped for unknown reason")
                        return ""
            
            return response.text or ""
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Gemini multi-file processing failed: {type(e).__name__}: {error_msg}", stack_info=True)
            # Check for specific error types and raise with appropriate message
            if "User location is not supported" in error_msg:
                raise ValueError("Gemini API error: User location is not supported for the API use. Please check your API region settings.")
            elif "400" in error_msg or "FAILED_PRECONDITION" in error_msg:
                raise ValueError(f"Gemini API configuration error: {error_msg}")
            elif "401" in error_msg or "403" in error_msg:
                raise ValueError(f"Gemini API authentication error: {error_msg}")
            elif "429" in error_msg:
                raise ValueError(f"Gemini API rate limit exceeded: {error_msg}")
            else:
                # Re-raise the original exception with context
                raise ValueError(f"Gemini API failed: {error_msg}") from e


class VisionProcessor:
    """Vision Processor - Uses OpenRouter to call vision models"""
    
    @staticmethod
    async def _process_pdf_with_openrouter(pdf_path: str, prompt: str, client: AsyncOpenAI, model: str) -> str:
        """
        Convert PDF to images using pypdfium2 and analyze with OpenRouter
        
        Args:
            pdf_path: PDF file path
            prompt: Analysis prompt
            client: OpenRouter client
            model: Model name
            
        Returns:
            Analysis result as JSON string
        """
        try:
            logging.info(f"Starting PDF processing: {pdf_path}")
            
            # Open PDF document with pypdfium2
            pdf = pdfium.PdfDocument(pdf_path)
            page_count = len(pdf)
            logging.info(f"PDF has {page_count} pages")
            
            all_results = []
            
            for page_num in range(page_count):
                logging.info(f"Processing page {page_num + 1}")
                page_start_time = time.time()
                
                # Load page
                page = pdf[page_num]
                
                # Render page to image with 1.5x scale (216 DPI = 72 * 3)
                # scale=1.5 gives good balance between quality and speed
                scale = 1.5
                bitmap = page.render(scale=scale)
                
                # Convert to PIL Image
                pil_image = bitmap.to_pil()
                
                # Convert to JPEG bytes
                img_buffer = io.BytesIO()
                pil_image.save(img_buffer, format="JPEG", quality=90)
                img_data = img_buffer.getvalue()
                
                # Optimize image
                optimized_img_data, stats = FileProcessor.optimize_image_for_llm(
                    img_data,
                    max_dimension=1536,
                    quality=85
                )
                
                # Convert to base64 for API call
                base64_image = base64.b64encode(optimized_img_data).decode('utf-8')
                
                image_conversion_time = time.time() - page_start_time
                logging.info(f"Image optimization and conversion took: {image_conversion_time:.2f}s, {stats}")
                
                # Create OpenRouter API message
                messages = [{
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
                        },
                        {
                            "type": "text", 
                            "text": f"{prompt}. Please return the result in JSON format."
                        }
                    ]
                }]
                
                try:
                    logging.info(f"Calling OpenRouter API (model: {model})...")
                    api_start_time = time.time()
                    
                    response = await client.chat.completions.create(
                        model=model,
                        messages=messages,
                        response_format={"type": "json_object"}
                    )
                    
                    api_duration = time.time() - api_start_time
                    page_total_time = time.time() - page_start_time
                    
                    logging.info(f"API request completed, took: {api_duration:.2f}s")
                    logging.info(f"Page {page_num + 1} total processing time: {page_total_time:.2f}s")
                    
                    result = response.choices[0].message.content
                    all_results.append({
                        'page': page_num + 1,
                        'content': result,
                        'api_duration': api_duration,
                        'image_conversion_time': image_conversion_time,
                        'page_total_time': page_total_time
                    })
                    
                    logging.info(f"Page {page_num + 1} analysis completed")
                    
                except Exception as api_error:
                    logging.error(f"Page {page_num + 1} API call failed: {api_error}")
                    all_results.append({
                        'page': page_num + 1,
                        'error': str(api_error)
                    })
            
            # Close document
            pdf.close()
            
            # Merge all page results
            if all_results:
                # Single page - return result directly
                if len(all_results) == 1 and 'content' in all_results[0]:
                    return clean_json_response(all_results[0]['content'])
                
                # Multiple pages - return first valid result
                for result in all_results:
                    if 'content' in result and result['content']:
                        return clean_json_response(result['content'])
            
            logging.warning("No valid analysis results obtained")
            return ""
            
        except Exception as e:
            logging.error(f"PDF processing error: {type(e).__name__}: {str(e)}", stack_info=True)
            return ""
    
    @staticmethod
    async def _process_image_with_openrouter(image_path: str, prompt: str, client: AsyncOpenAI, model: str) -> str:
        """
        Process image file directly and analyze with OpenRouter
        
        Args:
            image_path: Image file path
            prompt: Analysis prompt
            client: OpenRouter client
            model: Model name
            
        Returns:
            Analysis result as JSON string
        """
        try:
            logging.info(f"Starting image processing: {image_path}")
            start_time = time.time()
            
            # Read image file
            with open(image_path, "rb") as img_file:
                img_data = img_file.read()
            
            # Optimize image for faster processing
            optimized_img_data, stats = FileProcessor.optimize_image_for_llm(
                img_data,
                max_dimension=1536,
                quality=85
            )
            
            # Convert to base64
            base64_image = base64.b64encode(optimized_img_data).decode('utf-8')
            
            image_conversion_time = time.time() - start_time
            logging.info(f"Image optimization and conversion took: {image_conversion_time:.2f}s, {stats}")
            
            # Create OpenRouter API message
            messages = [{
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
                    },
                    {
                        "type": "text",
                        "text": f"{prompt}. Please return the result in JSON format."
                    }
                ]
            }]
            
            try:
                logging.info(f"Calling OpenRouter API (model: {model})...")
                api_start_time = time.time()
                
                response = await client.chat.completions.create(
                    model=model,
                    messages=messages,
                    response_format={"type": "json_object"}
                )
                
                api_duration = time.time() - api_start_time
                total_time = time.time() - start_time
                
                logging.info(f"API request completed, took: {api_duration:.2f}s")
                logging.info(f"Total image processing time: {total_time:.2f}s")
                
                result = response.choices[0].message.content
                logging.info("Image analysis completed")
                
                # Clean markdown code block markers from response
                return clean_json_response(result) if result else ""
                
            except Exception as api_error:
                logging.error(f"API call failed: {api_error}")
                return ""
                
        except Exception as e:
            logging.error(f"Image processing error: {type(e).__name__}: {str(e)}", stack_info=True)
            return ""

    @staticmethod
    def _build_prompt_with_schema(prompt: str, response_schema: Optional[Any] = None) -> str:
        """
        Embed response_schema into prompt for providers that don't support native schema (OpenRouter/Doubao)
        
        Args:
            prompt: Original prompt
            response_schema: Gemini format response_schema or JSON schema dict
            
        Returns:
            Complete prompt with schema instructions
        """
        if not response_schema:
            return prompt + "\n\nPlease return the result in JSON format."
        
        import json
        
        # Try to convert schema to JSON string
        try:
            if hasattr(response_schema, 'to_dict'):
                # Gemini schema object
                schema_dict = response_schema.to_dict()
            elif hasattr(response_schema, '__dict__'):
                schema_dict = response_schema.__dict__
            elif isinstance(response_schema, dict):
                schema_dict = response_schema
            else:
                schema_dict = str(response_schema)
            
            schema_str = json.dumps(schema_dict, indent=2, ensure_ascii=False)
            
            return f"""{prompt}

Please return the result in JSON format that strictly follows this schema:
```json
{schema_str}
```"""
        except Exception as e:
            logging.warning(f"Failed to serialize response_schema: {e}, using prompt without schema")
            return prompt + "\n\nPlease return the result in JSON format."

    @staticmethod
    async def vision_file_extract(
        local_file_path: str, 
        prompt: str = "Please extract all test indicators from this report and return the result in JSON format",
        model: str = "google/gemini-2.5-flash",
        client: Optional[AsyncOpenAI] = None,
        response_schema: Optional[Any] = None
    ) -> str:
        """
        Extract file content using vision model, supports PDF and image files
        
        Args:
            local_file_path: Local file path
            prompt: Extraction prompt
            model: Model name
            client: Client (optional, auto-created if not provided)
            response_schema: JSON schema (optional, embedded in prompt to guide output format)
            
        Returns:
            Analysis result as JSON string
        """
        try:
            file_path = pathlib.Path(local_file_path)
            if not file_path.exists():
                logging.error(f"File not found: {local_file_path}")
                return "" 
            
            # Get or create OpenRouter client
            if client is None:
                client = client_manager.get_async_openrouter_client()
            
            # Embed response_schema into prompt if provided
            final_prompt = VisionProcessor._build_prompt_with_schema(prompt, response_schema)
            
            # Check file type
            file_extension = file_path.suffix.lower()
            logging.info(f"Processing file: {local_file_path}, extension: {file_extension}")
            
            # Supported image formats
            image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp'}
            
            if file_extension == '.pdf':
                # PDF file - convert to images for processing
                return await VisionProcessor._process_pdf_with_openrouter(
                    pdf_path=str(file_path),
                    prompt=final_prompt, 
                    client=client,
                    model=model
                )
            elif file_extension in image_extensions:
                # Image file - process directly
                return await VisionProcessor._process_image_with_openrouter(
                    image_path=str(file_path),
                    prompt=final_prompt,
                    client=client,
                    model=model
                )
            else:
                # Unsupported file type
                logging.warning(f"Unsupported file type: {file_extension}")
                return ""
                
        except Exception as e:
            error_msg = str(e)
            logging.error(f"OpenRouter file extraction failed: {type(e).__name__}: {error_msg}", stack_info=True)
            raise ValueError(f"OpenRouter API failed: {error_msg}") from e

    @staticmethod
    async def _process_pdf_with_doubao(pdf_path: str, prompt: str, client: AsyncArk, model: str) -> str:
        """
        Convert PDF to images using pypdfium2 and analyze with Doubao
        
        Args:
            pdf_path: PDF file path
            prompt: Analysis prompt
            client: Doubao client
            model: Model name
            
        Returns:
            Analysis result as JSON string
        """
        try:
            logging.info(f"Starting PDF processing: {pdf_path}")
            
            # Open PDF document with pypdfium2
            pdf = pdfium.PdfDocument(pdf_path)
            page_count = len(pdf)
            logging.info(f"PDF has {page_count} pages")
            
            all_results = []
            
            for page_num in range(page_count):
                logging.info(f"Processing page {page_num + 1}")
                page_start_time = time.time()
                
                # Load page
                page = pdf[page_num]
                
                # Render page to image with 1.5x scale
                scale = 1.5
                bitmap = page.render(scale=scale)
                
                # Convert to PIL Image
                pil_image = bitmap.to_pil()
                
                # Convert to JPEG bytes
                img_buffer = io.BytesIO()
                pil_image.save(img_buffer, format="JPEG", quality=90)
                img_data = img_buffer.getvalue()
                
                # Optimize image
                optimized_img_data, stats = FileProcessor.optimize_image_for_llm(
                    img_data,
                    max_dimension=1536,
                    quality=85
                )
                
                # Convert to base64 for API call
                base64_image = base64.b64encode(optimized_img_data).decode('utf-8')
                
                image_conversion_time = time.time() - page_start_time
                logging.info(f"Image optimization and conversion took: {image_conversion_time:.2f}s, {stats}")
                
                # Create Doubao API message
                messages = [{
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
                        },
                        {
                            "type": "text", 
                            "text": f"{prompt}. Please return the result in JSON format."
                        }
                    ]
                }]
                
                try:
                    logging.info(f"Calling Doubao API (model: {model})...")
                    api_start_time = time.time()
                    
                    response = await client.chat.completions.create(
                        model=model,
                        messages=messages,
                        response_format={"type": "json_object"}
                    )
                    
                    api_duration = time.time() - api_start_time
                    page_total_time = time.time() - page_start_time
                    
                    logging.info(f"API request completed, took: {api_duration:.2f}s")
                    logging.info(f"Page {page_num + 1} total processing time: {page_total_time:.2f}s")
                    
                    result = response.choices[0].message.content
                    all_results.append({
                        'page': page_num + 1,
                        'content': result,
                        'api_duration': api_duration,
                        'image_conversion_time': image_conversion_time,
                        'page_total_time': page_total_time
                    })
                    
                    logging.info(f"Page {page_num + 1} analysis completed")
                    
                except Exception as api_error:
                    logging.error(f"Page {page_num + 1} API call failed: {api_error}")
                    all_results.append({
                        'page': page_num + 1,
                        'error': str(api_error)
                    })
            
            # Close document
            pdf.close()
            
            # Merge all page results
            if all_results:
                if len(all_results) == 1 and 'content' in all_results[0]:
                    return all_results[0]['content']
                
                for result in all_results:
                    if 'content' in result and result['content']:
                        return result['content']
            
            logging.warning("No valid analysis results obtained")
            return ""
            
        except Exception as e:
            logging.error(f"PDF processing error: {type(e).__name__}: {str(e)}", stack_info=True)
            return ""
    
    @staticmethod
    async def _process_image_with_doubao(image_path: str, prompt: str, client: AsyncArk, model: str) -> str:
        """
        Process image file directly and analyze with Doubao
        
        Args:
            image_path: Image file path
            prompt: Analysis prompt
            client: Doubao client
            model: Model name
            
        Returns:
            Analysis result as JSON string
        """
        try:
            logging.info(f"Starting image processing: {image_path}")
            start_time = time.time()
            
            # Read image file
            with open(image_path, "rb") as img_file:
                img_data = img_file.read()
            
            # Optimize image
            optimized_img_data, stats = FileProcessor.optimize_image_for_llm(
                img_data,
                max_dimension=1536,
                quality=85
            )
            
            # Convert to base64
            base64_image = base64.b64encode(optimized_img_data).decode('utf-8')
            
            image_conversion_time = time.time() - start_time
            logging.info(f"Image optimization and conversion took: {image_conversion_time:.2f}s, {stats}")
            
            # Create Doubao API message
            messages = [{
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
                    },
                    {
                        "type": "text",
                        "text": f"{prompt}. Please return the result in JSON format."
                    }
                ]
            }]
            
            try:
                logging.info(f"Calling Doubao API (model: {model})...")
                api_start_time = time.time()
                
                response = await client.chat.completions.create(
                    model=model,
                    messages=messages,
                    response_format={"type": "json_object"}
                )
                
                api_duration = time.time() - api_start_time
                total_time = time.time() - start_time
                
                logging.info(f"API request completed, took: {api_duration:.2f}s")
                logging.info(f"Total image processing time: {total_time:.2f}s")
                
                result = response.choices[0].message.content
                logging.info("Image analysis completed")
                
                return result or ""
                
            except Exception as api_error:
                logging.error(f"API call failed: {api_error}")
                return ""
                
        except Exception as e:
            logging.error(f"Image processing error: {type(e).__name__}: {str(e)}", stack_info=True)
            return ""

    @staticmethod
    async def doubao_file_extract(
        local_file_path: str, 
        prompt: str = "Please extract all test indicators from this report and return the result in JSON format",
        model: str = "doubao-1-5-ui-tars-250428",
        client: Optional[AsyncArk] = None
    ) -> str:
        """
        Doubao file extraction, supports PDF and image files
        
        Args:
            local_file_path: Local file path
            prompt: Extraction prompt
            model: Model name
            client: Doubao client (optional)
            
        Returns:
            Analysis result as JSON string
        """
        try:
            file_path = pathlib.Path(local_file_path)
            if not file_path.exists():
                logging.error(f"File not found: {local_file_path}")
                return "" 
            
            # Get or create Doubao client
            if client is None:
                volcengine_config = AIConfig.get_provider_config("volcengine")
                client = AsyncArk(
                    api_key=volcengine_config["api_key"],
                    base_url=volcengine_config["api_base"],
                )
            
            # Check file type
            file_extension = file_path.suffix.lower()
            logging.info(f"Processing file: {local_file_path}, extension: {file_extension}")
            
            # Supported image formats
            image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp'}
            
            if file_extension == '.pdf':
                return await VisionProcessor._process_pdf_with_doubao(
                    pdf_path=str(file_path),
                    prompt=prompt, 
                    client=client,
                    model=model
                )
            elif file_extension in image_extensions:
                return await VisionProcessor._process_image_with_doubao(
                    image_path=str(file_path),
                    prompt=prompt,
                    client=client,
                    model=model
                )
            else:
                logging.warning(f"Unsupported file type: {file_extension}")
                return ""
                
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Doubao file extraction failed: {type(e).__name__}: {error_msg}", stack_info=True)
            raise ValueError(f"Doubao API failed: {error_msg}") from e


# Exported file processing functions (directly use class methods to avoid redundant wrapping)
async def gemini_file_extract(
    file_path: str, content_type: str, prompt: str, config=None, model: str = "gemini-2.5-flash"
) -> str:
    """
    Extract file content using Gemini vision model
    
    Args:
        file_path: File path
        content_type: MIME type
        prompt: Extraction prompt
        config: Configuration parameters
        model: Model name
    """
    return await FileProcessor.gemini_file_extract(file_path, content_type, prompt, config, model)


async def gemini_multi_file_extract(
    files: List[Dict[str, str]], 
    prompt: str, 
    config=None, 
    model: str = "gemini-2.5-flash"
) -> str:
    """
    Extract content from multiple files using Gemini vision model
    
    Args:
        files: List of file dictionaries with 'path' and 'mime_type' keys
        prompt: Extraction prompt
        config: Configuration parameters
        model: Model name
    """
    return await FileProcessor.gemini_multi_file_extract(files, prompt, config, model)


async def doubao_file_extract(
    local_file_path: str, 
    prompt: str = "Please extract all test indicators from this report and return the result in JSON format",
    model: str = "doubao-1-5-ui-tars-250428",
    client: Optional[AsyncArk] = None
) -> str:
    """Doubao file extraction, supports PDF and image file analysis"""
    return await VisionProcessor.doubao_file_extract(
        local_file_path=local_file_path,
        prompt=prompt, 
        model=model,
        client=client
    )


async def unified_file_extract(
    file_path: str,
    prompt: str,
    content_type: str = "image/jpeg",
    model: Optional[str] = None,
    config: Optional[dict] = None,
    provider: Optional[str] = None
) -> str:
    """
    Unified file extraction entry point that automatically selects provider based on available API keys.
    
    Provider selection priority (based on configured API keys):
    1. gemini - If GOOGLE_API_KEY is configured
    2. openrouter - If OPENROUTER_API_KEY is configured
    3. doubao - If VOLCENGINE_API_KEY is configured
    
    JSON output comparison:
    - Gemini: Supports response_schema (enforced structure) + response_mime_type
    - OpenRouter: Supports response_format: {type: 'json_object'} (JSON mode, requires prompt to describe format)
    - Doubao: Supports response_format: {type: 'json_object'} (JSON mode, requires prompt to describe format)
    
    Notes:
    - config parameter (GenerateContentConfig) only works with Gemini native API
    - OpenRouter/Doubao automatically use JSON mode (response_format: json_object)
    - prompt should include clear JSON format requirements to ensure consistent output across providers
    
    Args:
        file_path: Path to the file
        prompt: Prompt for extraction (should include JSON format requirements)
        content_type: MIME type (only used for Gemini direct API)
        model: Model name (optional, auto-selects default based on provider if not provided)
        config: Gemini GenerateContentConfig (only works with Gemini, OpenRouter/Doubao use JSON mode automatically)
        provider: Force specific provider (optional, overrides auto-selection)
        
    Returns:
        Extracted file content (JSON string)
        
    Raises:
        ValueError: If no vision provider is available (no API keys configured)
        
    Reference:
        - OpenRouter JSON mode: https://openrouter.ai/docs/api/reference/overview
    """
    # Get provider config
    if provider:
        # Use specified provider
        provider_config = VisionProviderConfig.get_provider_by_name(provider)
        if not provider_config:
            raise ValueError(f"Unknown vision provider: {provider}. Available: gemini, openrouter, doubao")
        # Check if API key is configured for specified provider
        api_key = safe_read_cfg(provider_config["api_key_env"])
        if not api_key:
            raise ValueError(f"API key not configured for provider '{provider}' (env: {provider_config['api_key_env']})")
    else:
        # Auto-select based on available API keys
        provider_config = VisionProviderConfig.get_available_provider()
        if not provider_config:
            available_status = VisionProviderConfig.get_provider_status()
            raise ValueError(
                f"No vision provider available. Please configure one of the following API keys:\n"
                f"  - GOOGLE_API_KEY (for Gemini)\n"
                f"  - OPENROUTER_API_KEY (for OpenRouter)\n"
                f"  - VOLCENGINE_API_KEY (for Doubao)\n"
                f"Current status: {available_status}"
            )
    
    provider_name = provider_config["name"]
    actual_model = model or provider_config["default_model"]
    
    # Extract response_schema from Gemini config (for OpenRouter/Doubao)
    response_schema = None
    if config and hasattr(config, 'response_schema'):
        response_schema = config.response_schema
        if provider_name != "gemini":
            logging.info(f"📋 Embedding response_schema into prompt for {provider_name} provider")
    
    logging.info(f"unified_file_extract: Using {provider_name} provider with model {actual_model}")
    
    # Dispatch to appropriate handler
    if provider_name == "gemini":
        # Gemini native API - natively supports response_schema
        return await gemini_file_extract(
            file_path=file_path,
            content_type=content_type,
            prompt=prompt,
            config=config,
            model=actual_model
        )
    elif provider_name == "openrouter":
        # OpenRouter - JSON mode + embed schema in prompt
        return await VisionProcessor.vision_file_extract(
            local_file_path=file_path,
            prompt=prompt,
            model=actual_model,
            response_schema=response_schema
        )
    elif provider_name == "doubao":
        # Doubao - JSON mode + embed schema in prompt
        return await VisionProcessor.doubao_file_extract(
            local_file_path=file_path,
            prompt=VisionProcessor._build_prompt_with_schema(prompt, response_schema),
            model=actual_model
        )
    else:
        raise ValueError(f"Unsupported vision provider: {provider_name}")

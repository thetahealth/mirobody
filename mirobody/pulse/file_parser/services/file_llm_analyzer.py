"""
File content LLM analyzer service
"""

import asyncio
import json
import os
import tempfile
import time
from typing import Dict, Any, List, Optional, Tuple
import logging
from mirobody.utils.llm import batch_ai_response, unified_file_extract
from mirobody.utils.config import safe_read_cfg
from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService


def get_default_provider() -> tuple[str, str]:
    """
    Get default LLM provider based on environment.
    
    Returns:
        Tuple of (provider_name, display_name)
    """
    is_aliyun = safe_read_cfg("CLUSTER") == "ALIYUN"
    if is_aliyun:
        return "doubao-lite", "Doubao"
    return "openai", "OpenAI"


class FileLLMAnalyzer:
    """Service for analyzing file contents using LLM"""
    
    def __init__(self, provider: str = "openai"):
        """
        Initialize the LLM analyzer
        
        Args:
            provider: LLM provider (default: "openai")
        """
        self.provider = provider
        self.db_service = FileParserDatabaseService()
    
    async def analyze_files_with_extraction(
        self,
        files_data: List[Dict[str, Any]],
        user_id: str,
        msg_id: str,
        context: Optional[str] = None,
        language: str = "en"
    ) -> Dict[str, Any]:
        """
        Analyze multiple files using file extraction approach (Gemini/Doubao)
        Similar to abstract extraction but for multiple files
        
        Args:
            files_data: List of file data with raw content
            user_id: User ID  
            msg_id: Message ID
            context: Optional context for analysis
            language: User language preference (zh/en), defaults to "en"
            
        Returns:
            Analysis result
        """
        temp_files = []
        
        try:
            # Save files to temporary paths
            for file_data in files_data:
                filename = file_data.get("filename", "unknown")
                content = file_data.get("content", file_data.get("raw", b""))
                content_type = file_data.get("content_type", "application/octet-stream")
                
                # Handle byte content
                if not isinstance(content, (bytes, bytearray)):
                    content = content.encode('utf-8') if isinstance(content, str) else str(content).encode('utf-8')
                
                # Create temporary file
                suffix = os.path.splitext(filename)[1] if filename else ""
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
                    tmp_file.write(content)
                    temp_files.append((tmp_file.name, filename, content_type))
            
            # Build comprehensive prompt for multi-file analysis
            base_prompt = self._build_multi_file_extraction_prompt(
                temp_files=temp_files,
                context=context,
                language=language
            )
            
            # Process files concurrently using unified_file_extract
            semaphore = asyncio.Semaphore(3)  # Limit concurrent API calls
            
            async def process_single_file(temp_path, filename, content_type):
                """Process a single file using unified_file_extract with semaphore for rate limiting"""
                async with semaphore:
                    file_prompt = f"Analyzing file: {filename}\n\n{base_prompt}\n\nPlease provide a detailed analysis for this file with a warm, caring tone."
                    
                    result = await unified_file_extract(file_path=temp_path, prompt=file_prompt, content_type=content_type)
                return {"filename": filename, "analysis": result}
            
            # Process all files concurrently
            tasks = [process_single_file(temp_path, filename, content_type) for temp_path, filename, content_type in temp_files]
            start_time = time.time()
            individual_analyses = await asyncio.gather(*tasks, return_exceptions=True)
            
            logging.info(f"Processed {len(tasks)} files in {time.time() - start_time:.2f}s")
            
            # Handle exceptions
            for i, result in enumerate(individual_analyses):
                if isinstance(result, Exception):
                    logging.error(f"File {temp_files[i][1]} failed: {result}")
                    individual_analyses[i] = {"filename": temp_files[i][1], "analysis": f"Error: {str(result)}"}
            
            # Single file - return directly, multiple files - combine analyses
            if len(temp_files) == 1 and individual_analyses[0].get("analysis"):
                analysis_result = individual_analyses[0]["analysis"]
            else:
                analysis_result = await self._combine_analyses(individual_analyses, context, language)
            
            # Parse and structure the result
            structured_analysis = self._parse_analysis_response(analysis_result)
            
            # Save to database
            await self._save_analysis_to_db(msg_id, structured_analysis)
            
            return {
                "success": True,
                "analysis": structured_analysis.get("analysis", analysis_result),
                "summary": structured_analysis.get("summary", ""),
                "recommendations": structured_analysis.get("recommendations", []),
                "key_points": structured_analysis.get("key_points", []),
                "concerns": structured_analysis.get("concerns", []),
                "file_relationships": structured_analysis.get("file_relationships", []),
                "raw_response": analysis_result
            }
            
        except Exception as e:
            logging.error(f"Error in file extraction analysis: {str(e)}", stack_info=True)
            return {
                "success": False,
                "error": str(e)
            }
            
        finally:
            # Clean up temporary files
            for temp_path, _, _ in temp_files:
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass
    
    def _build_multi_file_extraction_prompt(
        self,
        temp_files: List[Tuple[str, str, str]],
        context: Optional[str] = None,
        language: str = "en"
    ) -> str:
        """
        Build prompt for multi-file extraction analysis
        
        Args:
            temp_files: List of (temp_path, filename, content_type) tuples
            context: Optional context
            language: User language preference
            
        Returns:
            Formatted prompt
        """
        file_count = len(temp_files)
        file_list = ", ".join([filename for _, filename, _ in temp_files])
        
        if language.lower() == "en":
            # English prompt
            if file_count > 1:
                base_prompt = f"""You are a gentle and caring AI assistant analyzing {file_count} files: {file_list}

I'll help you understand these files together. Please look for:
1. How these files connect and relate to each other
2. Any patterns or trends you might notice
3. Insights that come from looking at all files together
4. Important differences or consistencies between files

For health documents, I'll pay special attention to:
- Changes in health indicators over time
- How different test results might relate
- Your overall health picture based on all information"""
            else:
                base_prompt = f"""You are a gentle and caring AI assistant analyzing the file: {file_list}

I'll help you understand this file's content thoroughly and provide helpful insights."""
                
            if context:
                base_prompt += f"\n\nAdditional context you provided: {context}"
                
            base_prompt += """

Please respond in a warm, caring tone and structure your analysis in JSON format:
{
    "summary": "A gentle summary of your files and what I found",
    "analysis": "Detailed analysis with caring insights and explanations", 
    "recommendations": ["Kind, actionable suggestions for you"],
    "key_points": ["Important things I noticed that might help you"],
    "concerns": ["Things that might need attention (presented gently)"],
    "file_relationships": ["How your files connect and complement each other"]
}"""
        else:
            # Default prompt (English)
            if file_count > 1:
                base_prompt = f"""Hello, I'm your health assistant. I will analyze these {file_count} files for you: {file_list}

I will help you understand these files with a gentle approach, focusing on:
1. Connections and relationships between files
2. Patterns and trends in the data
3. Insights derived from all files combined
4. Important differences or consistencies across files

For health-related documents, I will pay special attention to:
- How health indicators change over time
- Relationships between different test results
- Overall health status based on all information"""
            else:
                base_prompt = f"""Hello, I'm your health assistant. I will analyze this file for you: {file_list}

I will help you understand this file's content with care and provide useful suggestions."""
                
            if context:
                base_prompt += f"\n\nAdditional context you provided: {context}"
                
            base_prompt += """

I will analyze with a warm, caring tone and provide results in the following JSON format:
{
    "summary": "A gentle summary of your file content and main findings",
    "analysis": "Detailed analysis with caring explanations",
    "recommendations": ["Gentle, practical suggestions for you"],
    "key_points": ["Important information I noticed that might help you"],
    "concerns": ["Areas that need attention (presented gently)"],
    "file_relationships": ["How your files connect and complement each other"]
}"""
        
        return base_prompt
    
    async def _combine_analyses(
        self,
        individual_analyses: List[Dict[str, Any]],
        context: Optional[str] = None,
        language: str = "en"
    ) -> str:
        """
        Combine individual file analyses into a comprehensive result
        
        Args:
            individual_analyses: List of individual file analysis results
            context: Optional context
            language: User language preference
            
        Returns:
            Combined analysis string
        """
        try:
            # Format all individual analyses
            analyses_text = []
            for item in individual_analyses:
                analyses_text.append(f"File: {item['filename']}\n{item['analysis']}\n")
            
            combined_text = "\n".join(analyses_text)
            
            # Use LLM to synthesize if we have multiple analyses
            if len(individual_analyses) > 1:
                if language.lower() == "en":
                    synthesis_prompt = f"""As a caring AI assistant, please integrate the following individual file analyses into a comprehensive, gentle analysis:

{combined_text}

{f'Additional context: {context}' if context else ''}

Please synthesize the information with a warm, caring tone following this JSON structure:
{{
    "summary": "A gentle overall summary of all files",
    "analysis": "Integrated analysis combining insights from all files with caring explanations",
    "recommendations": ["Kind, actionable recommendations"],
    "key_points": ["Important findings across all files that might help"],
    "concerns": ["Any concerns presented in a gentle, supportive way"],
    "file_relationships": ["How the files connect and complement each other"]
}}"""
                else:
                    synthesis_prompt = f"""As a caring health assistant, please integrate the following individual file analyses into a comprehensive, gentle analysis:

{combined_text}

{f'Additional context provided: {context}' if context else ''}

Please synthesize the information with a warm, caring tone following this JSON structure:
{{
    "summary": "A gentle overall summary of all files",
    "analysis": "Integrated analysis combining insights from all files with caring explanations",
    "recommendations": ["Gentle, practical recommendations for you"],
    "key_points": ["Important findings across all files that might help"],
    "concerns": ["Any concerns presented in a gentle, supportive way"],
    "file_relationships": ["How the files connect and complement each other"]
}}"""
                
                # Quick synthesis using batch_ai_response
                synthesis_provider, _ = get_default_provider()
                
                system_content = "You are a gentle and caring health assistant."
                response = await batch_ai_response(
                    messages=[
                        {"role": "system", "content": system_content},
                        {"role": "user", "content": synthesis_prompt}
                    ],
                    provider=synthesis_provider,
                    temperature=0.7,
                    max_tokens=2000
                )
                
                if response.get("content"):
                    return response["content"]
            
            return combined_text
            
        except Exception as e:
            logging.warning(f"Failed to combine analyses: {e}")
            return "\n".join([f"{item['filename']}: {item['analysis']}" for item in individual_analyses])
    
    async def analyze_files_content(
        self,
        files_data: List[Dict[str, Any]],
        user_id: str,
        msg_id: str,
        context: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Analyze multiple files content using LLM and generate a comprehensive response
        
        Args:
            files_data: List of file data with raw content extracted
            user_id: User ID
            msg_id: Message ID for the uploaded files
            context: Additional context for analysis
            
        Returns:
            Dict containing:
            - success: Whether analysis was successful
            - analysis: LLM analysis result
            - summary: Brief summary of files
            - recommendations: Any recommendations based on content
        """
        try:
            # Prepare content for analysis
            combined_content = self._prepare_files_content(files_data)
            
            if not combined_content:
                return {
                    "success": False,
                    "error": "No content available for analysis"
                }
            
            # Build the analysis prompt
            system_prompt = self._build_system_prompt()
            user_prompt = self._build_user_prompt(combined_content, context)
            
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            # Call LLM for analysis
            analysis_provider, _ = get_default_provider()
            
            response = await batch_ai_response(
                messages=messages,
                provider=analysis_provider,
                temperature=0.7,
                max_tokens=2000
            )
            
            if response.get("error"):
                logging.error(f"LLM analysis failed: {response.get('error')}")
                return {
                    "success": False,
                    "error": response.get("error")
                }
            
            analysis_result = response.get("content", "")
            
            # Parse structured response if possible
            structured_analysis = self._parse_analysis_response(analysis_result)
            
            # Save analysis result to database
            await self._save_analysis_to_db(msg_id, structured_analysis)
            
            return {
                "success": True,
                "analysis": structured_analysis.get("analysis", analysis_result),
                "summary": structured_analysis.get("summary", ""),
                "recommendations": structured_analysis.get("recommendations", []),
                "key_points": structured_analysis.get("key_points", []),
                "concerns": structured_analysis.get("concerns", []),
                "file_relationships": structured_analysis.get("file_relationships", []),
                "raw_response": analysis_result
            }
            
        except Exception as e:
            logging.error(f"LLM file analysis error: {e}", stack_info=True)
            return {"success": False, "error": str(e)}
    
    def _prepare_files_content(self, files_data: List[Dict[str, Any]]) -> str:
        """
        Prepare combined content from multiple raw files
        
        Args:
            files_data: List of file data with raw content
            
        Returns:
            Combined content string
        """
        combined_parts = []
        
        for file_data in files_data:
            filename = file_data.get("filename", "Unknown")
            
            # Get raw content - handle both byte and string formats
            content = file_data.get("content", file_data.get("raw", ""))
            
            # If content is bytes, decode it
            if isinstance(content, (bytes, bytearray)):
                try:
                    content = content.decode("utf-8", errors="replace")
                except Exception:
                    content = "[Binary content - unable to decode]"
            
            # Get file abstract if available
            abstract = file_data.get("file_abstract", "")
            
            # Get content type for context
            content_type = file_data.get("content_type", "unknown")
            
            if content:
                file_section = f"=== File: {filename} ===\n"
                file_section += f"Type: {content_type}\n"
                
                if abstract:
                    file_section += f"Abstract: {abstract}\n"
                
                # Limit content length per file to avoid token limits
                max_content_length = 3000
                if len(content) > max_content_length:
                    content = content[:max_content_length] + "... [content truncated]"
                file_section += f"Content:\n{content}\n"
                
                combined_parts.append(file_section)
        
        return "\n\n".join(combined_parts)
    
    def _build_system_prompt(self) -> str:
        """Build system prompt for file analysis"""
        return """You are a professional document analyst and health data expert. 
Your task is to analyze the uploaded files and provide comprehensive insights.

When analyzing files, you should:
1. Identify the type and purpose of each document
2. Extract key information and data points
3. **When multiple files are uploaded:**
   - Analyze them as a cohesive set, looking for connections and relationships
   - Identify patterns, trends, or correlations across different files
   - Provide integrated insights that consider all files together
   - Compare and cross-reference data between files when relevant
4. For health-related documents:
   - Highlight important health indicators and their values
   - Identify any abnormal or concerning findings
   - Note trends or patterns in the data across multiple reports/timeframes
   - Compare values across different test results or time periods
5. Provide actionable recommendations based on the combined content
6. Summarize the overall findings concisely, considering all files holistically

Please structure your response in the following JSON format:
{
    "summary": "Brief summary of all uploaded files and their combined insights",
    "analysis": "Detailed integrated analysis of all files, including relationships between documents, cross-file patterns, and comprehensive findings",
    "recommendations": ["List of actionable recommendations based on the combined analysis of all files"],
    "key_points": ["Important points or data extracted from the files, including cross-file insights"],
    "concerns": ["Any concerning findings that need attention, considering all files together"],
    "file_relationships": ["How the files relate to each other or complement each other's information"]
}

If the files contain health data, pay special attention to:
- Vital signs and their normal ranges
- Lab results and their clinical significance
- Medication information
- Diagnostic reports
- Trends over time across multiple reports
- Correlations between different health indicators in different files"""
    
    def _build_user_prompt(self, content: str, context: Optional[str] = None) -> str:
        """
        Build user prompt for analysis
        
        Args:
            content: Combined file content
            context: Additional context
            
        Returns:
            User prompt string
        """
        # Count the number of files from the content
        file_count = content.count("=== File:")
        
        if file_count > 1:
            prompt = f"Please analyze the following {file_count} uploaded files together as a cohesive set. Look for relationships, patterns, and correlations between the files:\n\n{content}"
        else:
            prompt = f"Please analyze the following uploaded file:\n\n{content}"
        
        if context:
            prompt += f"\n\nAdditional context: {context}"
        
        if file_count > 1:
            prompt += "\n\nImportant: Provide an integrated analysis that considers all files together, not separate analyses for each file. Focus on the combined insights and relationships between the files."
        
        prompt += "\n\nProvide a comprehensive analysis in the JSON format specified."
        
        return prompt
    
    def _parse_analysis_response(self, response: str) -> Dict[str, Any]:
        """
        Parse the LLM analysis response
        
        Args:
            response: Raw LLM response
            
        Returns:
            Parsed analysis dict
        """
        try:
            # Try to parse as JSON first
            if "{" in response and "}" in response:
                # Extract JSON from response
                start_idx = response.find("{")
                end_idx = response.rfind("}") + 1
                json_str = response[start_idx:end_idx]
                parsed = json.loads(json_str)
                return parsed
        except (json.JSONDecodeError, ValueError) as e:
            logging.warning(f"Failed to parse JSON from LLM response: {e}")
        
        # Fallback: return as unstructured analysis
        return {
            "analysis": response,
            "summary": "",
            "recommendations": [],
            "key_points": [],
            "concerns": [],
            "file_relationships": []
        }
    
    async def _save_analysis_to_db(self, msg_id: str, analysis: Dict[str, Any]) -> None:
        """Save LLM analysis to database"""
        try:
            await self.db_service.update_message_llm_analysis(msg_id=msg_id, llm_analysis=analysis)
        except Exception as e:
            logging.error(f"Failed to save LLM analysis: {e}", stack_info=True)


async def analyze_uploaded_files(
    files_data: List[Dict[str, Any]],
    user_id: str,
    msg_id: str,
    provider: str = "openai",
    context: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function to analyze uploaded files with LLM
    
    Args:
        files_data: List of processed file data
        user_id: User ID
        msg_id: Message ID
        provider: LLM provider
        context: Additional context
        
    Returns:
        Analysis result
    """
    analyzer = FileLLMAnalyzer(provider=provider)
    return await analyzer.analyze_files_content(
        files_data=files_data,
        user_id=user_id,
        msg_id=msg_id,
        context=context
    )


async def analyze_files_with_extraction(
    files_data: List[Dict[str, Any]],
    user_id: str,
    msg_id: str,
    provider: str = "openai",
    context: Optional[str] = None,
    language: str = "en"
) -> Dict[str, Any]:
    """
    Convenience function to analyze files using extraction approach (Gemini/Doubao)
    Similar to abstract extraction but supports multiple files
    
    Args:
        files_data: List of file data with raw content
        user_id: User ID
        msg_id: Message ID
        provider: LLM provider (for synthesis, actual extraction uses Gemini/Doubao based on env)
        context: Additional context
        language: User language preference (zh/en), defaults to "en"
        
    Returns:
        Analysis result with integrated insights from all files
    """
    analyzer = FileLLMAnalyzer(provider=provider)
    return await analyzer.analyze_files_with_extraction(
        files_data=files_data,
        user_id=user_id,
        msg_id=msg_id,
        context=context,
        language=language
    )


async def process_files_with_llm_analysis(
    files_data: List[Dict[str, Any]],
    user_id: str,
    msg_id: str,
    context: Optional[str] = None
):
    """
    Analyze raw files using file extraction approach (similar to abstract extraction)
    Uses Gemini or Doubao based on environment to directly process files
    
    Args:
        files_data: List of file data dictionaries containing raw content
        user_id: User ID
        msg_id: Message ID
        context: Optional context for LLM analysis
    """
    from datetime import datetime
    
    try:
        analyzer = FileLLMAnalyzer(provider="openai")
        analysis_result = await analyzer.analyze_files_with_extraction(
            files_data=files_data, user_id=user_id, msg_id=msg_id, context=context
        )
        
        if analysis_result.get("success"):
            # Trigger async file processing in background for storage
            from mirobody.pulse.file_parser.services.file_processing_service import process_files_async
            try:
                asyncio.create_task(process_files_async(files_data=files_data, user_id=user_id, msg_id=msg_id))
            except Exception:
                pass
        else:
            logging.error(f"LLM analysis failed for {msg_id}: {analysis_result.get('error')}")
            
    except Exception as e:
        logging.error(f"Error in LLM analysis for {msg_id}: {e}", stack_info=True)
        try:
            await FileParserDatabaseService.update_message_llm_analysis(
                msg_id=msg_id, llm_analysis={"status": "error", "error": str(e), "timestamp": datetime.now().isoformat()}
            )
        except Exception:
            pass

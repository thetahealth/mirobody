#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Enhanced Call Stack Analyzer

Intelligent call stack analysis tool for debugging and error analysis
- Auto-identify business code vs framework code
- Intelligently capture function parameters and context
- Multi-level formatted output
- Performance optimization and security limits

Usage examples:
    # Recommended - Business error debugging
    from utils.callstack_analyzer import get_business_call_chain
    try:
        # Business operation
        pass
    except Exception as e:
        error_info = get_business_call_chain(skip_frames=1, max_depth=8)
        logging.error(f"Operation failed: {e}\n{error_info}", stack_info=True)

    # Simple caller info (high performance)
    from utils.callstack_analyzer import get_simple_caller_info
    caller = get_simple_caller_info()

    # Deep debugging (includes framework calls)
    from utils.callstack_analyzer import get_debug_call_stack
    debug_info = get_debug_call_stack(max_depth=15)
"""

import logging
import json
import logging
import os
import time
from typing import Any, Dict, List


class EnhancedCallStackAnalyzer:
    """Enhanced call stack analyzer"""

    # Business-related file patterns, prioritize these calls
    BUSINESS_FILE_PATTERNS = [
        "routes/",
        "api/",
        "views/",
        "services/",
        "handlers/",
        "controllers/",
        "models/",
        "utils/",
        "core/",
        "business/",
        "app/",
        "src/",
        "lib/",
        "modules/",
        "components/",
    ]

    # Framework file patterns to ignore
    FRAMEWORK_PATTERNS = [
        "site-packages/",
        "asyncio/",
        "threading/",
        "concurrent/",
        "uvloop/",
        "fastapi/",
        "starlette/",
        "__pycache__/",
        "sqlalchemy/",
        "redis/",
        "pydantic/",
        "typing_extensions/",
        "multiprocessing/",
        "queue.py",
        "socketserver.py",
    ]

    @classmethod
    def get_enhanced_call_stack(cls, skip_frames: int = 0, max_depth: int = 10) -> Dict[str, Any]:
        """
        Get enhanced call stack information

        Args:
            skip_frames: Number of frames to skip
            max_depth: Maximum call depth

        Returns:
            Dict: Contains call chain, business context, performance info
        """
        import inspect

        stack = inspect.stack()
        relevant_stack = stack[skip_frames + 1 : skip_frames + 1 + max_depth]

        call_info = {
            "timestamp": time.time(),
            "business_chain": [],
            "full_chain": [],
            "context": {},
            "performance": {"call_depth": len(relevant_stack)},
        }

        business_calls = []
        all_calls = []

        for i, frame_info in enumerate(relevant_stack):
            filename = frame_info.filename
            function_name = frame_info.function
            line_number = frame_info.lineno

            # Simplify file path
            short_filename = os.path.basename(filename)
            relative_path = cls._get_relative_path(filename)

            frame_data = {
                "index": i + 1,
                "file": short_filename,
                "path": relative_path,
                "function": function_name,
                "line": line_number,
                "code": frame_info.code_context[0].strip() if frame_info.code_context else "",
                "is_business": cls._is_business_code(filename),
            }

            # Try to get local variables (business code only)
            if frame_data["is_business"]:
                frame_data["locals"] = cls._safe_get_locals(frame_info.frame)
                business_calls.append(frame_data)

            all_calls.append(frame_data)

        call_info["business_chain"] = business_calls
        call_info["full_chain"] = all_calls

        # Extract key context information
        if business_calls:
            call_info["context"] = cls._extract_context(business_calls)

        return call_info

    @classmethod
    def _get_relative_path(cls, filename: str) -> str:
        """Get relative path"""
        try:
            cwd = os.getcwd()
            if filename.startswith(cwd):
                return filename[len(cwd) :].lstrip("/")
            return filename
        except Exception as e:
            logging.error(f"Failed to get relative path: {e}")
            return filename

    @classmethod
    def _is_business_code(cls, filename: str) -> bool:
        """Check if it's business code"""
        # First check if it's framework code
        for pattern in cls.FRAMEWORK_PATTERNS:
            if pattern in filename:
                return False

        # Check if it's business code
        for pattern in cls.BUSINESS_FILE_PATTERNS:
            if pattern in filename:
                return True

        # If in current working directory, also consider it business code
        try:
            cwd = os.getcwd()
            return filename.startswith(cwd)
        except Exception as e:
            logging.error(f"Failed to check if business code: {e}")
            return True

    @classmethod
    def _safe_get_locals(cls, frame, max_vars: int = 5) -> Dict[str, str]:
        """Safely get local variables"""
        try:
            locals_dict = {}
            frame_locals = frame.f_locals

            # Filter out unimportant variables
            exclude_keys = {"self", "__builtins__", "__file__", "__name__", "cls"}

            count = 0
            for key, value in frame_locals.items():
                if count >= max_vars:
                    break
                if key.startswith("_") or key in exclude_keys:
                    continue

                try:
                    # Try to serialize value, limit length
                    str_value = str(value)
                    if len(str_value) > 100:
                        str_value = str_value[:100] + "..."
                    locals_dict[key] = str_value
                    count += 1
                except Exception as e:
                    logging.error(f"Failed to get local variable: {e}")
                    locals_dict[key] = "<Cannot serialize>"

            return locals_dict
        except Exception as e:
            logging.error(f"Failed to get local variables: {e}")
            return {}

    @classmethod
    def _extract_context(cls, business_calls: List[Dict]) -> Dict[str, Any]:
        """Extract business context"""
        context = {"entry_point": "", "key_functions": [], "parameters": {}}

        if business_calls:
            # Entry point is the last business call
            context["entry_point"] = f"{business_calls[-1]['file']}:{business_calls[-1]['function']}"

            # Key function chain
            context["key_functions"] = [f"{call['function']}({call['file']}:{call['line']})" for call in business_calls]

            # Parameter info (get from first call with parameters)
            for call in reversed(business_calls):
                if call.get("locals"):
                    context["parameters"] = call["locals"]
                    break

        return context

    @classmethod
    def format_for_logging(cls, call_info: Dict[str, Any], level: str = "error") -> str:
        """Format call info for logging"""
        if level == "error":
            return cls._format_error_log(call_info)
        elif level == "debug":
            return cls._format_debug_log(call_info)
        else:
            return cls._format_summary_log(call_info)

    @classmethod
    def _format_error_log(cls, call_info: Dict[str, Any]) -> str:
        """Format error-level log"""
        lines = []
        lines.append("🔍 === Call Stack Analysis ===")

        # Business call chain (focus)
        if call_info["business_chain"]:
            lines.append("\n📋 Business Call Chain:")
            for call in call_info["business_chain"]:
                lines.append(f"  [{call['index']}] {call['file']}:{call['line']} -> {call['function']}()")
                if call.get("locals"):
                    lines.append(f"      Params: {json.dumps(call['locals'], ensure_ascii=False)}")

        # Context information
        if call_info["context"]:
            context = call_info["context"]
            lines.append("\n🎯 Business Context:")
            lines.append(f"  Entry Point: {context.get('entry_point', 'Unknown')}")
            if context.get("parameters"):
                lines.append(f"  Key Params: {json.dumps(context['parameters'], ensure_ascii=False)}")

        # Performance info
        lines.append(f"\n⚡ Call Depth: {call_info['performance']['call_depth']}")

        return "\n".join(lines)

    @classmethod
    def _format_debug_log(cls, call_info: Dict[str, Any]) -> str:
        """Format debug-level log"""
        lines = []
        lines.append("🔧 === Detailed Call Stack ===")

        for call in call_info["full_chain"]:
            marker = "🏢" if call["is_business"] else "⚙️"
            lines.append(f"{marker} [{call['index']}] {call['path']}:{call['line']}")
            lines.append(f"    Function: {call['function']}()")
            lines.append(f"    Code: {call['code']}")
            if call.get("locals"):
                lines.append(f"    Variables: {json.dumps(call['locals'], ensure_ascii=False)}")
            lines.append("")

        return "\n".join(lines)

    @classmethod
    def _format_summary_log(cls, call_info: Dict[str, Any]) -> str:
        """Format summary-level log"""
        if call_info["business_chain"]:
            chain = " -> ".join(
                [f"{call['function']}({call['file']}:{call['line']})" for call in call_info["business_chain"]]
            )
            return f"Call Chain: {chain}"
        else:
            return f"Call Depth: {call_info['performance']['call_depth']}"


# Convenience functions
def get_enhanced_caller_info(skip_frames: int = 0) -> str:
    """Get enhanced caller information"""
    call_info = EnhancedCallStackAnalyzer.get_enhanced_call_stack(skip_frames + 1, max_depth=3)
    return EnhancedCallStackAnalyzer.format_for_logging(call_info, level="summary")


def get_business_call_chain(skip_frames: int = 0, max_depth: int = 8) -> str:
    """
    Get business call chain (for business logic debugging)

    Recommended error debugging function, focuses on business code call chain,
    filters out framework noise, provides parameter context information
    """
    call_info = EnhancedCallStackAnalyzer.get_enhanced_call_stack(skip_frames + 1, max_depth)
    return EnhancedCallStackAnalyzer.format_for_logging(call_info, level="error")


def get_debug_call_stack(skip_frames: int = 0, max_depth: int = 15) -> str:
    """
    Get detailed debug call stack (includes framework calls)

    For deep debugging, includes complete call stack information
    """
    call_info = EnhancedCallStackAnalyzer.get_enhanced_call_stack(skip_frames + 1, max_depth)
    return EnhancedCallStackAnalyzer.format_for_logging(call_info, level="debug")


def get_simple_caller_info(skip_frames: int = 0) -> str:
    """
    Get simple caller information (performance-optimized version)

    Only returns recent business caller info, suitable for high-frequency call scenarios
    """
    call_info = EnhancedCallStackAnalyzer.get_enhanced_call_stack(skip_frames + 1, max_depth=2)
    return EnhancedCallStackAnalyzer.format_for_logging(call_info, level="summary")

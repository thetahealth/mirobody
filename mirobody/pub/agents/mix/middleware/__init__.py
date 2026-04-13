"""
MixAgent Middlewares

- GenerateAnswerMiddleware: Signals end of data collection phase
"""

from .generate_answer import GenerateAnswerMiddleware, generate_answer

__all__ = ["GenerateAnswerMiddleware", "generate_answer"]

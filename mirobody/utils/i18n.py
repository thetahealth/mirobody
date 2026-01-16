"""
Multi-language internationalization module

Supports 5 languages: Chinese, English, French, Japanese, Spanish
Uses a design where one program file corresponds to one translation file for easy maintenance
Default language: English
"""

import logging
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict


class I18n:
    """Multi-language internationalization class"""

    # Language code mapping
    LANGUAGE_CODES = {
        "zh": "zh",
        "zh-cn": "zh",
        "zh_cn": "zh",
        "zh-hans": "zh",
        "zh_hans": "zh",
        "en": "en",
        "fr": "fr",
        "ja": "ja",
        "es": "es",
        "chinese": "zh",
        "english": "en",
        "french": "fr",
        "japanese": "ja",
        "spanish": "es",
    }

    def __init__(self):
        self._translations_cache = {}
        self._locales_dir = Path(__file__).parent.parent / "utils" / "locales"

    def clear_cache(self, module_name: str = None):
        """
        Clear translation cache

        Args:
            module_name: Specify module name, if None clears all cache
        """
        if module_name:
            self._translations_cache.pop(module_name, None)
        else:
            self._translations_cache.clear()

    def _load_translations(self, module_name: str) -> Dict[str, Any]:
        """
        Load translation file for specified module

        Args:
            module_name: Module name (corresponds to translation file name)

        Returns:
            Dict[str, Any]: Translation dictionary
        """
        if module_name in self._translations_cache:
            return self._translations_cache[module_name]

        translation_file = self._locales_dir / f"{module_name}.json"

        if not translation_file.exists():
            # If translation file doesn't exist, return empty dict
            self._translations_cache[module_name] = {}
            return {}

        try:
            with open(translation_file, "r", encoding="utf-8") as f:
                translations = json.load(f)
            self._translations_cache[module_name] = translations
            return translations
        except (json.JSONDecodeError, IOError) as e:
            # Return empty dict if loading fails
            print(f"Failed to load translations for {module_name}: {e}")
            self._translations_cache[module_name] = {}
            return {}

    def get_text(self, key: str, language: str = "en", module: str = None, **kwargs) -> str:
        """
        Get multi-language text

        Args:
            key: Text key
            language: Language code, default English
            module: Module name, if not provided will try to get from call stack
            **kwargs: Format parameters

        Returns:
            str: Text in corresponding language
        """
        # Normalize language code
        lang_code = self.LANGUAGE_CODES.get(language.lower(), "en")

        # If module not specified, try to get from call stack
        if module is None:
            import inspect

            frame = inspect.currentframe()
            try:
                # Get caller's filename as module name
                caller_frame = frame.f_back
                caller_filename = os.path.basename(caller_frame.f_code.co_filename)
                module = caller_filename.replace(".py", "")
            except Exception as e:
                logging.error(f"Failed to get module name: {e}")
                module = "default"
            finally:
                del frame

        # Load translations
        translations = self._load_translations(module)

        # Get text dictionary
        text_dict = translations.get(key, {})

        # Get text in corresponding language, priority: specified language -> English -> Chinese -> key itself
        text = text_dict.get(lang_code) or text_dict.get("en") or text_dict.get("zh") or key

        # Format text
        if kwargs:
            try:
                text = text.format(**kwargs)
            except (KeyError, ValueError):
                # Return original text if formatting fails
                pass

        return text

    def t(self, key: str, language: str = "en", module: str = None, **kwargs) -> str:
        """
        Shorthand for get_text
        """
        return self.get_text(key, language, module, **kwargs)


# Create global instance
i18n = I18n()


# Convenience functions
def t(key: str, language: str = "en", module: str = None, **kwargs) -> str:
    """
    Global translation function

    Args:
        key: Text key
        language: Language code, default English
        module: Module name, if not provided will automatically get from call stack
        **kwargs: Format parameters

    Returns:
        str: Text in corresponding language
    """
    return i18n.get_text(key, language, module, **kwargs)


def clear_translation_cache(module_name: str = None):
    """
    Convenience function to clear translation cache

    Args:
        module_name: Specify module name, if None clears all cache
    """
    i18n.clear_cache(module_name)


def debug_translation(key: str, language: str = "ja", module: str = "load_genetic_data"):
    """
    Debug translation function to check if translations are loaded correctly

    Args:
        key: Translation key
        language: Language code
        module: Module name

    Returns:
        str: Translation result
    """
    # Clear cache first
    clear_translation_cache(module)

    # Get translation again
    result = t(key, language, module)

    print(f"Debug translation: key='{key}', language='{language}', module='{module}', result='{result}'")

    return result

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from jinja2 import Environment, Template


class PromptResult:
    def __init__(
        self, template: Template, args: tuple[Any, ...], kwargs: dict[str, Any]
    ):
        self.template = template
        self.args = args
        self.kwargs = kwargs
        self._sync_result: Optional[str] = None

    def __str__(self) -> str:
        if self._sync_result is None:
            sync_env = Environment(loader=self.template.environment.loader)
            assert self.template.name is not None
            sync_template = sync_env.get_template(self.template.name)
            self._sync_result = sync_template.render(*self.args, **self.kwargs)
        return self._sync_result

    def __await__(self):
        async def _async_render():
            return await self.template.render_async(*self.args, **self.kwargs)

        return _async_render().__await__()

#-----------------------------------------------------------------------------

def load_prompt_templates() -> dict[str, str]:
    """
    Automatically load all .jinja files from the prompts directory.
    Returns: {filename (without .jinja): file content}
    """
    templates = {}
    prompts_dir = Path(__file__).parent
    
    try:
        for jinja_file in prompts_dir.glob("*.jinja"):
            try:
                template_name = jinja_file.stem
                template_content = jinja_file.read_text(encoding="utf-8")
                templates[template_name] = template_content
                logging.info(f"Loaded prompt template: {template_name}")
            except Exception as e:
                logging.warning(f"Failed to load prompt template {jinja_file.name}: {e}")
    except Exception as e:
        logging.error(f"Failed to scan prompts directory: {e}")
    
    return templates

#-----------------------------------------------------------------------------

class MirobodyPrompt:

    def __init__(self, template: str):
        self.template = Environment(enable_async=True).from_string(template)

    #-----------------------------------------------------

    def __call__(
        self,
        tools_description   : str = "",
        agent_name          : str = "",
        user_name           : str = "",
        current_time        : str = "",
        language            : str = "",
        **kargs
    ) -> PromptResult:
        return PromptResult(
            self.template,
            (),
            {
                "agent_name"        : agent_name if agent_name else "Mirobody",
                "user_name"         : user_name,
                "current_time"      : current_time if current_time
                                        else datetime.now().strftime("%A, %B %d, %Y, at %I:00 %p"),
                "language"          : language if language else "English",
                "tools_description" : tools_description,
            }
        )

#-----------------------------------------------------------------------------

__all__ = ["MirobodyPrompt", "load_prompt_templates"]
import logging
import importlib, importlib.util, inspect, logging, os

from types import ModuleType, FunctionType

from ..utils.plugin_dirs import import_plugin_module, resolve_plugin_dir

#-----------------------------------------------------------------------------

# Parameter names that mean "who is calling". Only `user_info` is injected from
# the verified JWT; the rest are the footgun this warns about.
_CALLER_IDENTITY_PARAMS = {"user_id", "userid", "uid", "current_user"}

# For MCP tools.
global_tools = {}
global_descriptions = []

# For LLM function calls.
global_openai_functions = []
global_openai_simplified_functions = []
global_gemini_functions = []

#-----------------------------------------------------------------------------

def _parse_type(s: str) -> tuple[str, str]:

    type_mapping = {
        "str"   : "string",
        "int"   : "integer",
        "float" : "number",
        "bool"  : "boolean"
    }

    #-----------------------------------------------------

    s = s.removeprefix("<class '").removesuffix("'>")

    if s in type_mapping:
        return type_mapping[s], ""

    #-----------------------------------------------------

    if s.startswith("typing.Optional[") and s.endswith("]"):
        s = s.removeprefix("typing.Optional[").removesuffix("]")

    if s in type_mapping:
        return type_mapping[s], ""

    #-----------------------------------------------------

    if s.startswith("dict[") or s.startswith("typing.Dict["):
        return "object", ""

    #-----------------------------------------------------

    if s.startswith("list[") and s.endswith("]"):
        s = s.removeprefix("list[").removesuffix("]")

        return "array", type_mapping[s] if s in type_mapping else "string"

    if s.startswith("typing.List[") and s.endswith("]"):
        s = s.removeprefix("typing.List[").removesuffix("]")

        return "array", type_mapping[s] if s in type_mapping else "string"

    #-----------------------------------------------------

    # Unknown type.
    return "string", ""

def parse_function(function: FunctionType) -> tuple[dict, bool, dict]:
    # 🆕 Check if function has custom inputSchema attribute
    if hasattr(function, 'inputSchema') and isinstance(function.inputSchema, dict):
        # Use custom inputSchema
        tool = {
            "name"          : function.__name__,
            "description"   : function.__doc__.strip().split('\n')[0] if function.__doc__ else "",
            "inputSchema"   : function.inputSchema,
            "annotations"   : {
                "title"             : function.__name__,
                "destructiveHint"   : False,
                "openWorldHint"     : False,
                "readOnlyHint"      : True,
            },
            # "_meta": {
            #     "openai/outputTemplate"     : "ui://widget/upload.html",
            #     "openai/toolInvocation/invoking": "Analyzing your health data",
            #     "openai/toolInvocation/invoked": "Served a health data analysis widget",
            #     "openai/widgetAccessible"   : True,
            #     "openai/resultCanProduceWidget": True,
            # },
        }
        if hasattr(function, 'meta'):
            tool["_meta"] = function.meta

        # Extract required_user_info from function signature
        require_user_info = "user_info" in function.__annotations__

        # Build parameters dictionary with all params and their defaults
        parameters = {}
        param_names = [k for k in function.__annotations__.keys() if k != "return" and k != "user_info"]

        if function.__defaults__:
            # Calculate how many params have defaults
            num_defaults = len(function.__defaults__)
            num_params = len(param_names)
            defaults_start = num_params - num_defaults

            # Set defaults for all params
            for i, param_name in enumerate(param_names):
                if i >= defaults_start:
                    # Has default value
                    parameters[param_name] = function.__defaults__[i - defaults_start]
                else:
                    # No default value (required param)
                    parameters[param_name] = None
        else:
            # No defaults, all params are required
            for param_name in param_names:
                parameters[param_name] = None

        return tool, require_user_info, parameters

    # Original auto-generation logic
    tool = {
        "name"          : function.__name__,
        "description"   : "",
        "inputSchema" : {
            "type"  : "object",
            "properties": {

            },
            "required"  : []
        },
        "annotations"   : {
            "title"             : function.__name__,
            "destructiveHint"   : False,
            "openWorldHint"     : False,
            "readOnlyHint"      : True,
        },
        # "_meta": {
        #     "openai/outputTemplate"     : "ui://widget/upload.html",
        #     "openai/toolInvocation/invoking": "Analyzing your health data",
        #     "openai/toolInvocation/invoked": "Served a health data analysis widget",
        #     "openai/widgetAccessible"   : True,
        #     "openai/resultCanProduceWidget": True,
        # },
    }
    if hasattr(function, 'meta'):
        tool["_meta"] = function.meta

    #-----------------------------------------------------

    param_size_bias = 0
    if "self" in function.__annotations__:
        param_size_bias = 1

    optional_param_size = len(function.__defaults__) if function.__defaults__ else 0
    param_size          = len(function.__annotations__) - param_size_bias # No "self", but including "outputSchema".
    required_param_size = param_size - optional_param_size
    param_cnt           = 0
    require_user_info   = False
    parameters          = {}

    # ── input schema, generated by the official MCP SDK ──────────────────────
    #
    # This used to be a hand-rolled loop over `function.__annotations__` that
    # matched the *stringified* annotation against a table of type names. It got
    # four cases wrong, verified: a bare `dict` became "string"; `Literal[...]`
    # and `Enum` silently lost their constraint, so the model could send any
    # string and the schema would not object; and `Annotated[int, Field(...)]`
    # was read as "string", discarding both the type and the bounds.
    #
    # `func_metadata` is the SDK's own generator (Pydantic underneath), so we
    # inherit correct handling of those plus defaults, Optional, nested models
    # and $defs. `skip_names` is exactly our hidden-argument mechanism: the
    # server fills `user_info` from the caller's JWT, and it must never appear
    # in the schema the model sees.
    require_user_info = "user_info" in function.__annotations__
    parameters = {name: None for name in function.__annotations__ if name != "return"}

    # A tool that takes `user_id` instead of `user_info` is not authenticated and
    # does not know it. `user_info` is the injection hook; anything else stays in
    # the schema, so the MODEL supplies it — and an MCP client can then ask for
    # another person's data by sending a different id. The main README told
    # authors to do exactly this until it was corrected, so warn rather than
    # assume nobody followed it.
    if not require_user_info:
        caller_ident = _CALLER_IDENTITY_PARAMS & set(function.__annotations__)
        if caller_ident:
            logging.warning(
                "tool %r takes %s but no `user_info`: the server cannot inject the "
                "caller's identity, so that parameter stays in the tool schema and "
                "the model supplies it. Take `user_info: Dict[str, Any]` instead.",
                getattr(function, "__qualname__", function.__name__),
                ", ".join(sorted(caller_ident)),
            )

    from mcp.server.mcpserver.utilities.func_metadata import func_metadata

    meta = func_metadata(function, skip_names=["user_info", "self"])
    schema = meta.arg_model.model_json_schema(by_alias=True)
    tool["inputSchema"] = {
        "type": "object",
        "properties": schema.get("properties", {}),
    }
    if schema.get("required"):
        tool["inputSchema"]["required"] = list(schema["required"])
    if schema.get("$defs"):
        tool["inputSchema"]["$defs"] = schema["$defs"]

    #-----------------------------------------------------
    # User's descriptions.

    # 0: function description.
    # 1: argument description.
    # 2: return description.
    # 3: exception description.
    line_type = 0
    current_arg_key = ""
    returns_lines: list[str] = []

    for line in function.__doc__.splitlines():

        #-------------------------------------------------
        # Ignore empty line.

        line = line.strip()
        if not line:
            continue

        #-------------------------------------------------
        # Update line type.

        lower = line.lower()
        if lower in ("arg:", "args:", "argument:", "arguments:", "param:", "params:", "parameter:", "parameters:"):
            line_type = 1
            current_arg_key = ""
            continue

        elif lower in ("return:", "returns:", "result:", "results:"):
            line_type = 2
            continue

        #-------------------------------------------------
        # Save descriptions.

        # Argument.
        if line_type == 1:
            try:
                pos = line.find(":")
                if pos > 0:
                    key     = line[:pos].strip()
                    value   = line[pos+1:].strip()

                    if key and key in tool["inputSchema"]["properties"]:
                        tool["inputSchema"]["properties"][key]["description"] = value
                        current_arg_key = key
                        continue

                # Continuation line for current argument's description.
                if current_arg_key and current_arg_key in tool["inputSchema"]["properties"]:
                    desc = tool["inputSchema"]["properties"][current_arg_key].get("description", "")
                    tool["inputSchema"]["properties"][current_arg_key]["description"] = desc + "\n" + line if desc else line

            except Exception as e:
                logging.warning(str(e), extra={"line": line})

        # Return value — and anything after it (Notes, caveats, examples).
        #
        # This used to be `pass`: everything from `Returns:` onward was parsed
        # and thrown away. Tool descriptions are prompt engineering, not
        # documentation — the model only ever sees `description` plus the
        # per-argument strings, so a documented return shape and any
        # "Notes for LLMs" guidance never reached it, and the model had to guess
        # what a tool gives back. Collected here and appended to the
        # description below, under their original heading.
        elif line_type == 2:
            returns_lines.append(line)

        # Exception.
        elif line_type == 3:
            try:
                pos = line.find(":")
                if pos > 0:
                    key     = line[:pos].strip()
                    value   = line[pos+1:].strip()

                    if key and value:
                        if "exception" not in tool:
                            tool["exception"] = {}

                        tool["exception"][key] = value

            except Exception as e:
                logging.warning(str(e), extra={"line": line})

        # Function.
        else:
            if len(tool["description"]) > 0:
                tool["description"] += "\n"

            tool["description"] += line

    #-----------------------------------------------------
    # Fold the Returns section (and any trailing guidance) back into the one
    # field the model actually reads.

    if returns_lines:
        if tool["description"]:
            tool["description"] += "\n\n"
        tool["description"] += "Returns:\n" + "\n".join(returns_lines)

    #-----------------------------------------------------

    # Clean up empty required list that some APIs (e.g. Gemini) don't accept.
    schema = tool.get("inputSchema", {})
    if "required" in schema and not schema["required"]:
        del schema["required"]

    return tool, require_user_info, parameters

#-----------------------------------------------------------------------------

def load_tools_from_class(klass, module_name: str) -> dict:
    # If the class defines a _enabled() static method, call it to check
    # whether this service should be registered (e.g. API key availability).
    if hasattr(klass, "_enabled") and callable(klass._enabled):
        try:
            if not klass._enabled():
                logging.info(f"Skipping disabled tool class: {klass.__name__}")
                return {}
        except Exception as e:
            logging.warning(f"Error checking _enabled for {klass.__name__}: {e}")
            return {}

    try:
        functions = inspect.getmembers(klass, predicate=inspect.isfunction)
    except Exception as e:
        logging.warning(f"Error getting tool functions: {e}")
        return {}

    #-----------------------------------------------------

    tools           = {}
    class_instance  = klass()

    for function_name, function in functions:
        if inspect.isabstract(function) or inspect.isbuiltin(function):
            continue

        #-------------------------------------

        # Private method.
        if function_name.startswith("_"):
            continue

        # Method declared in base classes.
        if function.__qualname__:
            logging.debug(f"{function.__qualname__} in {klass.__name__}")

            a = function.__qualname__.split(".")
            if len(a) > 0:
                if a[0] != klass.__name__:
                    logging.debug(f"Ignore tool method in another class: {function_name}")
                    continue
        else:
            logging.info(f"no qualname for {function_name}")

        # Imported method.
        if module_name and module_name != function.__module__:
            logging.debug(f"Ignore tool method in another module: {function_name}")
            continue

        #-------------------------------------

        tool_description, require_user_info, parameters = parse_function(function)

        tools[function_name] = {
            "description"   : tool_description,
            "auth"          : require_user_info,
            "instance"      : getattr(class_instance, function_name),
            "parameters"    : parameters,
        }

        logging.info(f"Loaded tool: {function_name}")

    return tools

#-----------------------------------------------------------------------------

def load_tools_from_module(module: ModuleType, module_name: str) -> dict:

    tools = {}

    #-----------------------------------------------------
    # Parse classes.

    try:
        classes = inspect.getmembers(module, predicate=inspect.isclass)
    except Exception as e:
        logging.warning(f"Error getting tool classes: {e}")
        classes = {}

    for class_name, klass in classes:
        if inspect.isabstract(klass) or \
            inspect.isbuiltin(klass) or \
            class_name == "Any" or \
            not class_name.endswith("Service"):

            logging.debug(f"Ignore class: {class_name}")
            continue

        class_tools = load_tools_from_class(klass, module_name)
        if class_tools:
            tools[class_name] = class_tools

    #-----------------------------------------------------
    # Parse functions.

    try:
        functions = inspect.getmembers(module, predicate=inspect.isfunction)
    except Exception as e:
        logging.warning(f"Error getting tool functions: {e}")
        functions = {}

    module_tools = {}

    for function_name, function in functions:
        if function_name.startswith("_") or \
            inspect.isabstract(function) or \
            inspect.isbuiltin(function) or \
            function.__module__ != module_name:

            logging.debug(f"Ignore function: {function_name}")
            continue

        tool_description, require_user_info, parameters = parse_function(function)

        module_tools[function_name] = {
            "description"   : tool_description,
            "auth"          : require_user_info,
            "instance"      : function,
            "parameters"    : parameters,
        }

        logging.info(f"Loaded tool: {function_name}")

    if module_tools:
        tools[module_name] = module_tools

    #-----------------------------------------------------

    return tools

#-----------------------------------------------------------------------------

def load_tools_from_directory(dir: str) -> tuple[dict, list]:
    # `MCP_TOOL_DIRS` is a documented extension point ("add your own directory"),
    # so a directory OUTSIDE the package has to work. It did not: the old rule
    # derived the module name from the path string, which only doubles as a
    # dotted package for paths under CWD spelled exactly that way. See
    # utils/plugin_dirs.py for what that cost.
    target_directory, module_name_prefix = resolve_plugin_dir(dir)

    if not target_directory:
        logging.warning(f"No tool directory found at {dir!r}")
        return {}, []

    #-----------------------------------------------------

    logging.debug(f"Loading tools from {target_directory}")

    try:
        entries = os.scandir(target_directory)
    except Exception as e:
        logging.warning(f"Error scanning tool directory {target_directory}: {e}")
        return {}, []

    #-----------------------------------------------------

    tools       = {}
    descriptions= []

    for entry in entries:
        if entry.is_dir() or \
            not entry.name.lower().endswith(".py") or \
            entry.name.startswith("_"):
            continue

        try:
            module_name, imported_module = import_plugin_module(
                target_directory, module_name_prefix, entry.name
            )
        except Exception as e:
            logging.warning(f"Error importing tool module {entry.name} from {target_directory}: {e}")
            continue

        #-------------------------------------------------

        module_tools = load_tools_from_module(imported_module, module_name)
        if not module_tools:
            continue

        for class_name in module_tools:
            class_tools = module_tools[class_name]
            if not class_tools or not isinstance(class_tools, dict):
                continue

            for tool_name in class_tools:
                tool_info = class_tools[tool_name]
                if not isinstance(tool_info, dict):
                    continue

                if "description" not in tool_info:
                    continue

                descriptions.append(tool_info["description"])

                tools[tool_name] = tool_info

    #-----------------------------------------------------

    if tools:
        global global_tools
        global_tools.update(tools)

    if descriptions:
        global global_descriptions
        global_descriptions.extend(descriptions)

        global global_openai_functions
        global global_openai_simplified_functions
        global global_gemini_functions
        for description in descriptions:
            global_openai_functions.append(
                {
                    "type"      : "function",
                    "function"  : {
                        "name"          : description["name"],
                        "description"   : description["description"],
                        "parameters"    : description["inputSchema"]
                    }
                }
            )
            global_openai_simplified_functions.append(
                {
                    "type"          : "function",
                    "name"          : description["name"],
                    "description"   : description["description"],
                    "parameters"    : description["inputSchema"]
                }
            )
            global_gemini_functions.append(
                {
                    "name"          : description["name"],
                    "description"   : description["description"],
                    "parameters"    : description["inputSchema"]
                }
            )

    return tools, descriptions

#-----------------------------------------------------------------------------

def load_tools_from_directories(dirs: list[str]) -> tuple[dict, list]:
    tools       = {}
    descriptions= []

    for dir in dirs:
        if not dir:
            continue

        cur_tools, cur_descriptions = load_tools_from_directory(dir)
        if cur_tools:
            tools.update(cur_tools)
            descriptions.extend(cur_descriptions)

    return tools, descriptions

#-----------------------------------------------------------------------------

async def call_tool(tools: dict, tool_name: str, arguments: dict | None = None, user_id: str = "", session_id: str = ""):
    """Invoke one loaded tool by name.

    Unknown argument names are rejected rather than ignored. The generated
    JSON Schema governs what the model *sees*; nothing enforced it at call
    time, so `keyword=` instead of `keywords=` used to be dropped on the floor
    and the tool ran on its defaults — returning a confident, wrong answer that
    the model had no way to attribute to its own typo. An error naming the
    accepted parameters is something it can act on.

    Returns None only when the tool cannot be dispatched at all (unknown name,
    no bound instance); otherwise a result, or `{"success": False, "error": …}`.
    """
    # The following two scenarios should never occur.
    if not tools or \
        not tool_name or \
        tool_name not in tools:

        return None

    tool = tools[tool_name]
    if "instance" not in tool or \
        not tool["instance"]:

        return None

    #-----------------------------------------------------
    # Prepare arguments.

    kwargs = tool["parameters"].copy() if "parameters" in tool else {}
    if arguments:
        unknown = [k for k in arguments if k not in kwargs]
        if unknown:
            accepted = ", ".join(sorted(kwargs)) or "(none)"
            error = (
                f"Unknown argument(s) for {tool_name}: {', '.join(sorted(unknown))}. "
                f"Accepted: {accepted}."
            )
            logging.warning(error)
            return {
                "success"   : False,
                "error"     : error
            }

        for k in arguments:
            kwargs[k] = arguments[k]
    if "auth" in tool and tool["auth"]:
        kwargs["user_info"] = {
            "success": True,
            "user_id": user_id,
            "session_id": session_id
        }

    #-----------------------------------------------------
    # Invoke the function.

    try:
        if inspect.iscoroutinefunction(tool["instance"]):
            result = await tool["instance"](**kwargs)

        else:
            result = tool["instance"](**kwargs)

    except Exception as e:
        logging.error(str(e))

        return {
            "success"   : False,
            "error"     : str(e)
        }

    return result


async def call_global_tool(tool_name: str, arguments: dict | None = None, user_id: str = "", session_id: str = ""):
    global global_tools
    return await call_tool(global_tools, tool_name=tool_name, arguments=arguments, user_id=user_id, session_id=session_id)


def get_global_tool_count() -> int:
    global global_tools
    return len(global_tools)


def get_global_descriptions() -> list:
    """The tool schemas exactly as a client receives them in `tools/list`.

    Kept as public API even though nothing inside the package calls it: for a
    project whose MCP tool surface IS the product, this is the only way to see
    what a model actually gets — description text, generated inputSchema and
    all. It is the check that caught the docstring parser silently dropping
    every `Returns:` section, and the four types the old schema generator got
    wrong. Removing it once cost real debugging time; don't remove it again.
    """
    global global_descriptions
    return global_descriptions


def get_global_tools() -> dict:
    global global_tools
    return global_tools



def get_global_functions(style: str="") -> list:
    if style == "openai":
        global global_openai_functions
        return global_openai_functions

    elif style == "gemini":
        global global_gemini_functions
        return global_gemini_functions

    else:
        global global_openai_simplified_functions
        return global_openai_simplified_functions


#-----------------------------------------------------------------------------

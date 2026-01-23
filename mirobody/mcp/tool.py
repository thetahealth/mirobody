import importlib, importlib.util, inspect, logging, os

from types import ModuleType, FunctionType

#-----------------------------------------------------------------------------

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

    for param_name in function.__annotations__:
        param_cnt += 1

        # This is the return value of function.
        if param_name == "return":
            # tool["outputSchema"]["type"] = type_name
            break

        parameters[param_name] = None

        # This argument should be filled by MCP server according to client's JWT token.
        if param_name == "user_info":
            require_user_info = True
                # param_cnt <= required_param_size

            continue

        #-------------------------------------------------

        type_name, items_type_name = _parse_type(str(function.__annotations__[param_name]))

        # No type name found.
        #   This scenario will not occur yet.
        if len(type_name) == 0:
            continue

        # Add a parameter straightly.
        tool["inputSchema"]["properties"][param_name] = {
            "type": type_name
        }

        # This parameter is a list, fill its item type.
        if len(items_type_name) > 0:
            tool["inputSchema"]["properties"][param_name]["items"] = {
                "type": items_type_name
            }

        #-------------------------------------------------

        if param_cnt < required_param_size:
            tool["inputSchema"]["required"].append(param_name)

        elif param_cnt <= required_param_size + optional_param_size:
            if function.__defaults__ is not None:
                try:
                    default_value = function.__defaults__[param_cnt - required_param_size]

                    # Description.
                    tool["inputSchema"]["properties"][param_name]["default"] = default_value

                    # Callable.
                    parameters[param_name] = default_value

                except Exception as e:
                    logging.warning(str(e))


    #-----------------------------------------------------

    if not function.__doc__ or not isinstance(function.__doc__, str):
        return tool, require_user_info, parameters

    #-----------------------------------------------------
    # User's descriptions.

    # 0: function description.
    # 1: argument description.
    # 2: return description.
    # 3: exception description.
    line_type = 0

    for line in function.__doc__.splitlines():

        #-------------------------------------------------
        # Ignore empty line.

        line = line.strip()
        if not line:
            continue

        #-------------------------------------------------
        # Update line type.

        lower = line.lower()
        if lower == "args:":
            line_type = 1
            continue

        elif lower == "returns:":
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

            except Exception as e:
                logging.warning(str(e), extra={"line": line})

        # Return value.
        elif line_type == 2:
            pass
            # if len(tool["outputSchema"]["description"]) > 0:
            #     tool["outputSchema"]["description"] += "\n"

            # tool["outputSchema"]["description"] += line

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

    return tool, require_user_info, parameters

#-----------------------------------------------------------------------------

def load_tools_from_class(klass, module_name: str) -> dict:
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
        if inspect.isabstract(function) or \
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
    target_directory = dir.strip()
    if not target_directory:
        return {}, []

    target_directory = target_directory.removeprefix(os.getcwd())
    target_directory = target_directory.removeprefix(os.sep)
    target_directory = target_directory.strip()

    if not target_directory:
        return {}, []

    #-----------------------------------------------------

    module_name_prefix = target_directory.replace(os.path.sep, ".")

    if not os.path.isdir(target_directory):
        try:
            spec = importlib.util.find_spec(module_name_prefix)
        except:
            spec = None

        if not spec or not spec.origin:
            logging.warning(f"No tool found from {module_name_prefix}")
            return {}, []

        target_directory = os.path.dirname(spec.origin)

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

        module_name = module_name_prefix + "." + entry.name[0:len(entry.name)-3]
        logging.info(module_name)

        try:
            imported_module = importlib.import_module(module_name)
        except Exception as e:
            logging.warning(f"Error importing tool module {module_name}: {e}")
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

async def call_tool(tools: dict, tool_name: str, arguments: dict | None = None, user_id: str = ""):
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
        for k in arguments:
            if k in kwargs:
                kwargs[k] = arguments[k]

    if "auth" in tool and tool["auth"]:
        kwargs["user_info"] = {
            "success": True,
            "user_id": user_id
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


async def call_global_tool(tool_name: str, arguments: dict | None = None, user_id: str = ""):
    global global_tools
    return await call_tool(global_tools, tool_name=tool_name, arguments=arguments, user_id=user_id)


def get_global_tool_count() -> int:
    global global_tools
    return len(global_tools)


def get_global_tools() -> dict:
    global global_tools
    return global_tools


def get_global_descriptions() -> list:
    global global_descriptions
    return global_descriptions


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

import importlib, importlib.util, inspect, logging, os

from types import ModuleType
from typing import Any, AsyncGenerator, Callable

from ..utils import Config, global_config

#-----------------------------------------------------------------------------

class AbstractAgent:
    def __init__(self, **kwargs):
        pass

    async def generate_response(self, *args: Any, **kwargs: Any) -> AsyncGenerator[dict[str, Any], None]: ...

    @staticmethod
    def load_llm_clients(llm_client_config: dict[str, Any]) -> dict[str, Any]: ...

#-----------------------------------------------------------------------------

global_agents = {}
global_public_agents = {}

#-----------------------------------------------------------------------------

def load_agents_from_module(module: ModuleType, module_name: str, config: Config = None) -> dict:
    try:
        classes = inspect.getmembers(module, predicate=inspect.isclass)
    except Exception as e:
        logging.warning(f"Error getting agent classes: {e}")
        return {}

    #-----------------------------------------------------

    agents = {}

    for class_name, klass in classes:
        if inspect.isabstract(klass) or \
            inspect.isbuiltin(klass) or \
            class_name == "Any" or \
            module_name and module_name != klass.__module__:

            logging.debug(f"Ignore agent class: {class_name}")
            continue

        try:
            functions = inspect.getmembers(klass, predicate=inspect.isfunction)
        except Exception as e:
            logging.warning(f"Error getting agent functions: {e}")
            continue

        #-------------------------------------------------

        load_llm_clients_func = None

        for function_name, function in functions:
            if function_name in ["generate_response"]:
                agent_name = class_name.strip().removesuffix("Agent").strip()
                agents[agent_name] = klass

                logging.info(f"Loaded agent: {agent_name}")
                if load_llm_clients_func is not None:
                    break

            elif function_name == "load_llm_clients":
                load_llm_clients_func = function

        if config and callable(load_llm_clients_func):
            err = load_llm_clients_for_agent_class(agent_name, config, load_llm_clients_func)
            if err:
                logging.warning(err)

    return agents

#-----------------------------------------------------------------------------

def load_agents_from_directory(dir: str, private: bool = False, config: Config = None) -> dict:
    target_directory = dir.strip()
    if not target_directory:
        return {}
    
    target_directory = target_directory.removeprefix(os.getcwd())
    target_directory = target_directory.removeprefix(os.sep)
    target_directory = target_directory.strip()

    if not target_directory:
        return {}
    
    #-----------------------------------------------------

    module_name_prefix = target_directory.replace(os.path.sep, ".")

    if not os.path.isdir(target_directory):
        try:
            spec = importlib.util.find_spec(module_name_prefix)
        except:
            spec = None

        if not spec or not spec.origin:
            logging.warning(f"No agent found from {module_name_prefix}")
            return {}

        target_directory = os.path.dirname(spec.origin)

    #-----------------------------------------------------

    logging.debug(f"Loading agents from {target_directory}")

    try:
        entries = os.scandir(target_directory)
    except Exception as e:
        logging.warning(f"Error scanning agent directory {target_directory}: {e}")
        return {}
    
    #-----------------------------------------------------

    agents = {}

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
            logging.warning(f"Error importing agent module {module_name}: {e}")
            continue

        #-------------------------------------------------

        module_agents = load_agents_from_module(imported_module, module_name, config)
        if not module_agents:
            continue

        agents.update(module_agents)

    #-----------------------------------------------------

    if agents:
        global global_agents
        global_agents.update(agents)

        if not private:
            global global_public_agents
            global_public_agents.update(agents)

    return agents

#-----------------------------------------------------------------------------

def load_agents_from_directories(dirs: list[str], private: bool = False, config: Config = None) -> dict:
    agents = {}

    for dir in dirs:
        if not dir:
            continue

        dir_agents = load_agents_from_directory(dir, private, config)
        if dir_agents:
            agents.update(dir_agents)

    return agents

#-----------------------------------------------------------------------------

def get_global_agent_count() -> int:
    global global_agents
    return len(global_agents)


global_public_agent_names = None
global_all_agent_names = None


def get_global_agents(public: bool = False) -> list[str]:
    global global_llm_clients_for_agents
    if not global_llm_clients_for_agents:
        return []

    #-----------------------------------------------------

    global global_public_agent_names
    global global_all_agent_names

    if public:
        if global_public_agent_names is not None:
            return global_public_agent_names
    else:
        if global_all_agent_names is not None:
            return global_all_agent_names

    #-----------------------------------------------------

    global global_agents
    global global_public_agents

    agents = global_public_agents if public else global_agents

    agent_names = []
    for agent_name in agents:
        llm_clients = global_llm_clients_for_agents.get(agent_name)
        if llm_clients:
            agent_names.append(agent_name)

    if public:
        global_public_agent_names = agent_names
    else:
        global_all_agent_names = agent_names

    #-----------------------------------------------------

    return agent_names


def get_agent(agents: dict, agent_name: str, **kwargs) -> AbstractAgent | None:
    agent_class = agents.get(agent_name)
    if not agent_class:
        return None
    
    if isinstance(agent_class, str):
        agent_class = agents.get(agent_class)
        if not agent_class:
            return None

    return agent_class(**kwargs)


def get_global_agent(agent_name: str, **kwargs) -> AbstractAgent | None:
    config = global_config()

    if not config:
        options = kwargs
    else:
        options = config.get_options_for_agent(agent_name=agent_name)
        if not options or not isinstance(options, dict):
            options = kwargs
        else:
            options.update(kwargs)

    global global_agents
    return get_agent(agents=global_agents, agent_name=agent_name, **options)

#-----------------------------------------------------------------------------

global_llm_clients_for_agents = {}
global_llm_client_names = None
global_agents_with_llm_client_names = None


def load_llm_clients_for_agent_class(
    agent_name: str,
    config: Config,
    loader_func: Callable[[dict[str, Any]], dict[str, Any]]
) -> str | None:

    logging.info(f"loading LLM clients for agent: {agent_name}")

    if not agent_name:
        return "Invalid agent name."
    if not config:
        return "Invalid config."

    llm_clients = {}

    agent_config = None
    if config:
        agent_config = config.get_options_for_agent(agent_name)

    llm_client_config = None
    if agent_config:
        llm_client_config = agent_config.get("providers")
    else:
        logging.warning("Empty agent config.")

    if llm_client_config:
        clients = loader_func(llm_client_config)
        if clients is not None:
            llm_clients = clients
        else:
            logging.warning("No LLM client loaded.")
    else:
        logging.warning("Empty LLM client config.")

    global global_llm_clients_for_agents
    global_llm_clients_for_agents[agent_name] = llm_clients

    logging.info(f"Loaded {len(llm_clients)} LLM clients for agent {agent_name}.")

    return None


def get_agents_with_llm_client_names() -> list[str]:
    global global_agents_with_llm_client_names
    if global_agents_with_llm_client_names is not None:
        return global_agents_with_llm_client_names

    #-----------------------------------------------------

    global global_llm_clients_for_agents
    if not global_llm_clients_for_agents:
        return []

    global global_public_agents
    if not global_public_agents:
        return []

    a = []
    for agent_name, llm_clients in global_llm_clients_for_agents.items():
        if agent_name not in global_public_agents or not llm_clients:
            continue

        for llm_client_name, llm_client_instance in llm_clients.items():
            a.append(f"{agent_name}/{llm_client_name}")

    a.sort()

    global_agents_with_llm_client_names = a
    return global_agents_with_llm_client_names


def get_llm_client_by_name(agent_name: str, provider_name: str) -> Any:
    global global_llm_clients_for_agents
    if not global_llm_clients_for_agents:
        return None

    llm_clients = global_llm_clients_for_agents.get(agent_name)
    if not llm_clients or not isinstance(llm_clients, dict):
        return None

    return llm_clients.get(provider_name)

#-----------------------------------------------------------------------------

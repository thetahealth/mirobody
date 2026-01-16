import importlib.util, json, logging, os

#-----------------------------------------------------------------------------

global_resources = {}

#-----------------------------------------------------------------------------

def load_resources_from_directory(dir: str) -> tuple[dict, list]:
    target_directory = dir.strip()
    if not target_directory:
        return {}, []
    
    target_directory = target_directory.removeprefix(os.getcwd())
    target_directory = target_directory.removeprefix(os.sep)
    target_directory = target_directory.strip()

    if not target_directory:
        return {}, []

    #-----------------------------------------------------

    if not os.path.isdir(target_directory):
        module_name_prefix = target_directory.replace(os.path.sep, ".")

        try:
            spec = importlib.util.find_spec(module_name_prefix)
        except Exception as e:
            spec = None

        if not spec or not spec.submodule_search_locations:
            logging.warning(f"No resource found from {module_name_prefix}")
            return {}, []

        target_directory = spec.submodule_search_locations[0]

    logging.debug(f"Loading resources from {target_directory}")

    #-----------------------------------------------------

    try:
        entries = os.scandir(target_directory)
    except Exception as e:
        logging.warning(f"Error scanning resource directory {target_directory}: {e}")
        return {}, []
    
    #-----------------------------------------------------

    resource_map    = {}
    resource_list   = []

    for entry in entries:
        if entry.is_dir() or not entry.name.lower().endswith(".json"):
            continue

        try:
            meta_filename = os.path.join(target_directory, entry.name)
            with open(meta_filename, encoding="utf-8") as meta_file:
                resource = json.load(meta_file)

            if not resource or not isinstance(resource, dict):
                continue

            if "uri" not in resource or not isinstance(resource["uri"], str) or not resource["uri"]:
                continue

            #---------------------------------------------

            if "_meta" in resource and isinstance(resource["_meta"], dict):
                resource["_meta"]["openai/outputTemplate"]          = resource["uri"]
                resource["_meta"]["openai/widgetAccessible"]        = True
                resource["_meta"]["openai/resultCanProduceWidget"]  = True

                if "annotations" not in resource["_meta"] or not isinstance(resource["_meta"]["annotations"], dict):
                    resource["_meta"]["annotations"] = {
                        "destructiveHint"   : False,
                        "openWorldHint"     : False,
                        "readOnlyHint"      : True
                    }

            #---------------------------------------------

            if "text" not in resource:
                if "_file" in resource and isinstance(resource["_file"], str) and len(resource["_file"]) > 0:
                    content_filename = os.path.join(target_directory, resource["_file"])
                    with open(content_filename, encoding="utf-8") as content_file:
                        resource["text"] = content_file.read()

            if "_file" in resource:
                del resource["_file"]

            #---------------------------------------------

            resource_map[resource["uri"]] = resource.copy()

            if "text" in resource:
                del resource["text"]

            resource_list.append(resource)

        except Exception as e:
            logging.warning(f"Failed to load resource '{meta_filename}': {str(e)}")
            continue

        logging.info(f"Loaded resource '{meta_filename}'.")

    #-----------------------------------------------------

    return resource_map, resource_list

#-----------------------------------------------------------------------------

def load_resources_from_directories(dirs: list[str]) -> tuple[dict, list]:
    resource_map    = {}
    resource_list   = []
    
    for dir in dirs:
        if not dir:
            continue

        cur_resource_map, cur_resources = load_resources_from_directory(dir)
        if cur_resource_map:
            resource_map.update(cur_resource_map)
        if cur_resources:
            resource_list.extend(cur_resources)

    return resource_map, resource_list

#-----------------------------------------------------------------------------

def read_resource(resources: dict, uri: str) -> dict | None:
    if not resources or not isinstance(resources) or \
        not uri or not isinstance(uri, str):
        return None
    
    if uri not in resources:
        return None
    
    return resources[uri]


def read_global_resource(uri: str) -> dict | None:
    global global_resources
    return read_resource(global_resources, uri)

#-----------------------------------------------------------------------------

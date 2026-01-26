import sys

#-----------------------------------------------------------------------------

async def main():
    yaml_filenames: list[str] = []
    if len(sys.argv) > 1:
        yaml_filenames.extend(sys.argv[1:])
    else:
        print(f"Usage: python {sys.argv[0]} [config_files]\n")

    # TODO: Create your own FastAPI routers.
    fastapi_routers = []

    from mirobody.server import Server
    await Server.start(yaml_files=yaml_filenames, fastapi_routers=fastapi_routers)

#-----------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())

#-----------------------------------------------------------------------------

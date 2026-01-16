import asyncio, sys

from mirobody.server import Server

#-----------------------------------------------------------------------------

async def main():
    yaml_filenames: list[str] = []
    if len(sys.argv) > 1:
        yaml_filenames.extend(sys.argv[1:])
    else:
        print(f"Usage: python {sys.argv[0]} [config_files]\n")

    await Server.start(yaml_filenames)

#-----------------------------------------------------------------------------

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())

#-----------------------------------------------------------------------------

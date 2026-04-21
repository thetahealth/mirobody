import sys

#-----------------------------------------------------------------------------

async def main():
    yaml_filenames: list[str] = []
    if len(sys.argv) > 1:
        yaml_filenames.extend(sys.argv[1:])
    else:
        print(f"Usage: python {sys.argv[0]} [config_files]\n")

    from mirobody.server import Worker
    await Worker.start(yaml_files=yaml_filenames)

#-----------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())

#-----------------------------------------------------------------------------

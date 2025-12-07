from mirobody.server import Server
from mirobody.utils import Config


#-----------------------------------------------------------------------------

async def main():
    yaml_filenames = ['config/config.yaml']

    config = await Config.init(yaml_filenames=yaml_filenames)
    
    # Initialize database schema before starting server
    # pg_config = config.get_postgresql()
    # db_init_success = await initialize_database(pg_config, enable_idempotency=False)
    # if not db_init_success:
    #     print("Warning: Database initialization encountered errors. Check logs for details.")

    await Server.start(yaml_filenames)

#-----------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

#-----------------------------------------------------------------------------

from db import DB
from asyncpg import Record


async def create_version_table() -> None:
    query = """CREATE TABLE IF NOT EXISTS migration_versions(
        version varchar not null
    )"""
    await DB.conn.execute(query)


async def insert_version(version_id: str) -> None:
    query = f"""INSERT INTO migration_versions values ('{version_id}')"""
    await DB.conn.execute(query)


async def delete_last_version() -> None:
    query = f"""DELETE FROM migration_versions"""
    await DB.conn.execute(query)


async def empty_table() -> bool:
    query = """SELECT not exists(
        SELECT FROM migration_versions
    )"""
    return await DB.conn.fetchval(query)


async def function_exists() -> bool:
    query = """
        SELECT exists(
            SELECT * 
            FROM pg_proc 
            WHERE proname = 'generate_create_table_statement'
        )
    """
    return await DB.conn.fetchval(query)

#TODO: SELECT dependence on data types
# where numeric -> numeric_version
async def get_db_info() -> Record:
    query = """SELECT cl.table_name, column_name, data_type, is_nullable,
                      numeric_scale, numeric_precision, 
	                  character_maximum_length, column_default
               FROM information_schema.columns cl
               JOIN information_schema.tables tb
               ON cl.table_name = tb.table_name
               WHERE tb.table_schema='public'
               AND tb.table_type='BASE TABLE'
        """
    return await DB.conn.fetch(query)


async def get_last_version() -> list:
    query = """SELECT version 
               FROM migration_versions"""
    return await DB.conn.fetchval(query)

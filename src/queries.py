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


async def get_db_info() -> Record:
    query = """SELECT cl.table_name, column_name, data_type, is_nullable
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

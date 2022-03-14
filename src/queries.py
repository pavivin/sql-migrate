from db import DB
from typing import List, Mapping


async def create_version_table() -> None:
    query = """
        CREATE TABLE IF NOT EXISTS migration_versions(
            version varchar PRIMARY KEY
        )
    """
    await DB.conn.execute(query)


async def insert_version(version_id: str) -> None:
    query = f"""
        INSERT INTO migration_versions (version)
        VALUES ('{version_id}')
    """
    await DB.conn.execute(query)


async def update_version(version_id: str) -> None:
    query = f"""
        UPDATE migration_versions 
            SET version = '{version_id}'
    """
    await DB.conn.execute(query)


async def delete_last_version() -> None:
    query = """
        DELETE 
        FROM migration_versions
    """
    await DB.conn.execute(query)


async def get_version_table() -> bool:
    query = """
        SELECT version
        FROM migration_versions
    """
    return await DB.conn.fetchval(query)


# TODO: SELECT dependence on data types
# where numeric -> numeric_version
async def get_db_info() -> List[Mapping]:
    query = """
        SELECT cl.table_name, column_name, udt_name as data_type, is_nullable,
            character_maximum_length, column_default
        FROM information_schema.columns cl
        JOIN information_schema.tables tb
            ON cl.table_name = tb.table_name
        WHERE tb.table_schema='public'
            AND tb.table_type='BASE TABLE'
    """
    return await DB.conn.fetch(query)


async def get_constraints() -> List[Mapping]:
    query = """
        SELECT c.column_name, c.data_type, c.table_name, constraint_name, constraint_type
        FROM information_schema.table_constraints tc 
        JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
        JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
            AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
    """
    return await DB.conn.fetch(query)


async def get_last_version() -> str:
    query = """
        SELECT version
        FROM migration_versions
    """
    return await DB.conn.fetchval(query)

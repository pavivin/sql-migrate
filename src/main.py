import asyncpg
import os
import uuid

from db import DB
from queries import (
    create_version_table,
    delete_last_version,
    get_version_table,
    get_db_info,
    get_last_version,
    insert_version,
    update_version,
)
from query import MigrateQuery
from record import JsonRecord


async def first_migration():
    version_id = uuid.uuid4().hex
    await create_version_table()

    db_info = await get_db_info()
    json_record = JsonRecord.aggregate_db_info(db_info)
    query = await MigrateQuery.query_from_json(json_record)

    os.makedirs("migrations", exist_ok=True)
    MigrateQuery.create_query(version_id, query)

    os.makedirs("records", exist_ok=True)
    JsonRecord.create_record(version_id, json_record)

    await insert_version(version_id)


async def migrate(version_id: str):
    db_info = await get_db_info()
    cur_record = JsonRecord.aggregate_db_info(db_info)
    last_version = await get_last_version()
    last_record = JsonRecord.record_from_file(last_version)

    diff = JsonRecord.get_record_diff(cur_record, last_record)

    if diff:
        JsonRecord.create_record(version_id, diff)
        query = await MigrateQuery.query_from_json(diff)
        MigrateQuery.create_query(version_id, query)
        await delete_last_version()
        await update_version(version_id)


async def main():
    await DB.connect()

    try:
        version_id = await get_version_table()
        if version_id:
            await migrate(version_id)
        else:
            await first_migration()
    except asyncpg.exceptions.PostgresError:
        await first_migration()
    

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

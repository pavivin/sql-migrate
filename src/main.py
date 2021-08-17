import os
import uuid
from typing import Dict

import ujson
from asyncpg import Record

from db import DB
from queries import (
    create_version_table, delete_last_version,
    empty_table, get_db_info,
    get_last_version, insert_version
)


def aggregate_db_info(db_info: Record) -> Dict[list, dict]:
    record_json = {}

    for row in db_info:
        table_name = row['table_name']
        column_name = row['column_name']
        data_type = row['data_type']
        is_nullable = 'NULL' if row['is_nullable'] == 'YES' else 'NOT NULL'
        record_json[table_name] = record_json.get(table_name, {})
        record_json[table_name][column_name] = {
            'data_type': data_type,
            'is_nullable': is_nullable
        }

    return record_json


def query_from_json(record: Dict[list, dict]) -> str:
    query = ''
    for tablename, column in record.items():

        table_query = f'CREATE TABLE {tablename} (\n\t'

        columns_query = ''
        column_len = len(column)

        for i, _column in enumerate(column.items()):
            column_name, column_value = _column
            data_type = column_value['data_type']
            is_nullable = column_value['is_nullable']

            columns_query = ''.join((columns_query, column_name))
            columns_query = ' '.join((columns_query, data_type, is_nullable))
            if i != column_len - 1:
                columns_query = ''.join((columns_query, ',\n\t'))

        query = ''.join((query, table_query, columns_query, '\n);\n\n'))

    return query


def create_record(version_id: str, record: dict) -> None:
    with open(f'records/{version_id}.json', 'w+') as record_file:
        record_file.write(ujson.dumps(record))


def create_query(version_id: str, query: str) -> None:
    with open(f'migrations/{version_id}.sql', 'w+') as migration_file:
        migration_file.write(query)


def get_diff(cur_record: dict, last_record: dict) -> dict:
    diff = {}
    for table_name, table_value in last_record.items():
        diff[table_name] = diff.get(table_name, {})
        for column_name, column_value in table_value.items():
            if cur_record[table_name].get(column_name):
                # TODO: get difference from alter table and drop table
                column_diff = dict(set(column_value) ^ set(cur_record[table_name][column_name]))
            else:
                diff[table_name][column_name] = column_value
            if column_diff:
                diff[table_name][column_name] = column_diff

        if not diff[table_name]:
            del diff[table_name]
    return diff


async def first_migration(version_id: str):
    await create_version_table()

    db_info = await get_db_info()
    record = aggregate_db_info(db_info)
    query = query_from_json(record)

    os.makedirs('.migrations', exist_ok=True)
    create_query(version_id, query)

    os.makedirs('.records', exist_ok=True)
    create_record(version_id, record)

    await insert_version(version_id)


async def migrate(version_id: str):
    db_info = await get_db_info()
    cur_record = aggregate_db_info(db_info)
    last_version = await get_last_version()
    with open(f'records/{last_version}.json', 'r') as record_file:
        last_record = ujson.loads(record_file.read())

    diff = get_diff(cur_record, last_record)

    if diff:
        create_record(version_id, diff)
        query = query_from_json(diff)
        create_query(version_id, query)
        await delete_last_version()
        await insert_version(version_id)


async def main():
    await DB.connect()

    is_empty = await empty_table()

    version_id = uuid.uuid4().hex

    if is_empty:
        await first_migration(version_id)
    else:
        await migrate(version_id)


if __name__ == "__main__":
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())

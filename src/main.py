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

        record_json[table_name] = record_json.get(table_name, [])
        record_json[table_name].append({
            'column_name': column_name,
            'data_type': data_type,
            'is_nullable': is_nullable
        })

    return record_json


def query_from_json(record: Dict[list, dict]) -> str:
    query = ''
    for tablename, column in record.items():

        table_query = f'CREATE TABLE {tablename} (\n\t'

        columns_query = ''
        column_len = len(column)

        for i, row in enumerate(column):
            column_name = row['column_name']
            data_type = row['data_type']
            is_nullable = row['is_nullable']

            columns_query = ''.join((columns_query, column_name))
            columns_query = ' '.join((columns_query, data_type, is_nullable))
            if i != column_len - 1:
                columns_query = ''.join((columns_query, ',\n\t'))

        query = ''.join((query, table_query, columns_query, '\n);\n\n'))

    return query


async def get_db_conn(args):
    options = {
        'database': args.postgres_database,
        'user': args.postgres_user,
        'host': args.postgres_host,
        'port': args.postgres_port
    }


async def migrate():
    ...


def create_record(version_id: str, record: dict) -> None:
    with open(f'records/{version_id}.json', 'w+') as record_file:
        record_file.write(ujson.dumps(record))


def create_query(version_id: str, query: str) -> None:
    with open(f'migrations/{version_id}.sql', 'w+') as migration_file:
        migration_file.write(query)


async def main():
    await DB.connect()

    await create_version_table()

    is_empty = await empty_table()

    version_id = uuid.uuid4().hex

    if is_empty:

        db_info = await get_db_info()
        record = aggregate_db_info(db_info)
        query = query_from_json(record)

        os.makedirs('migrations', exist_ok=True)
        create_query(version_id, query)

        os.makedirs('records', exist_ok=True)
        create_record(version_id, record)

        await insert_version(version_id)

    else:

        db_info = await get_db_info()
        cur_record = aggregate_db_info(db_info)
        last_version = await get_last_version()
        with open(f'records/{last_version}.json', 'r') as record_file:
            last_record = ujson.loads(record_file.read())

        diff = {}
        for table_name, table_value in last_record.items():
            # table_diff = dict(set(table_value) ^ set(cur_record[table_name]))
            # if table_name:
            #     diff[table_name] = table_diff
            for column in table_value:
                ...
                # column_diff = dict(set(column) ^ set(cur_record[table_name][column]))
                # if column_diff:
                #     diff[table_name] = diff.get(table_name, [])
                #     diff[table_name].append([column])
        if diff:
            create_record(version_id, cur_record)
            query = query_from_json(cur_record)
            create_query(version_id, query)
            await delete_last_version()


if __name__ == "__main__":
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())

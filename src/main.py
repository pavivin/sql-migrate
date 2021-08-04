from typing import Dict
from db import DB
from asyncpg import Record


async def get_db_info() -> Record:
    query = """SELECT cl.table_name, column_name, data_type, is_nullable
                FROM information_schema.columns cl
                join information_schema.tables tb
                on cl.table_name = tb.table_name
                WHERE tb.table_schema='public'
                AND tb.table_type='BASE TABLE'
        """
    return await DB.conn.fetch(query)


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
        
        table_query = f'CREATE TABLE {tablename} ('

        columns_query = ''

        column_len = len(column)

        for i, row in enumerate(column):
            column_name = row['column_name']
            data_type = row['data_type']
            is_nullable = row['is_nullable']
            
            columns_query = ' '.join((columns_query, column_name, data_type, is_nullable))
            if i != column_len - 1:
                columns_query = ''.join((columns_query, ','))

        query = ''.join((query, table_query, columns_query, ');'))

    return query


async def main():
    await DB.connect()

    db_info = await get_db_info()
    record = aggregate_db_info(db_info)
    query = query_from_json(record)

    print(query)


if __name__ == "__main__":
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())

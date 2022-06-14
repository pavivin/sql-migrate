from typing import Dict, List, Mapping
from dataclasses import dataclass
from queries import get_contraints

# TODO: rewrite


def find_constraint(contraints: List[Mapping], tablename: str):
    return [
        item for item in contraints
        if item['table_name'] == tablename
        and item['constraint_type'] in {'PRIMARY KEY', 'UNIQUE'}
    ]


@dataclass
class Row:
    data_type: str
    is_nullable: str  # naming
    char_len: str
    column_default: str


@dataclass
class Column:
    table_schema: str
    constraint_name: str
    table_name: str
    column_name: str
    foreign_table_schema: str
    foreign_table_name: str
    foreign_column_name: str
    constraint_type: str


class MigrateQuery:
    @staticmethod
    async def query_from_json(record: Dict[list, dict]) -> str:
        query = ""

        constraints = await get_contraints()

        for tablename, column in record.items():

            table_query = f"CREATE TABLE {tablename} (\n\t"

            columns_query = ""
            column_len = len(column)

            _constraints = find_constraint(constraints, tablename)
            for item in _constraints:
                # TODO: naming
                item_record = Column(**item)
                columns_query = "".join(
                    (columns_query, f'CONSTRAINT {item_record.constraint_name} {item_record.constraint_type} ({item_record.column_name})', ",\n\t"))

            for i, _column in enumerate(column.items()):
                # TODO: rewrite without wildcard
                # TODO: reorganize row and column
                column_name, column_value = _column
                row = Row(**column_value)

                columns_query = "".join((columns_query, column_name)).strip()
                columns_query = " ".join((columns_query, row.data_type, row.char_len, row.is_nullable, row.column_default)).strip()
                if i != column_len - 1:
                    columns_query = "".join((columns_query, ",\n\t"))

            query = "".join((query, table_query, columns_query, "\n);\n\n"))

        return query

    @staticmethod
    def create_query(version_id: str, query: str) -> None:
        with open(f"migrations/{version_id}.sql", "w+") as migration_file:
            migration_file.write(query)

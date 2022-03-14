from typing import Dict, List, Mapping

from queries import get_constraints

# TODO: rewrite


def find_constraint(contraints: List[Mapping], tablename: str):
    ans = []
    for item in contraints:
        if item['table_name'] == tablename and item['constraint_type'] in('PRIMARY KEY', 'UNIQUE'):
            ans.append(item)

    return ans


class MigrateQuery:
    @staticmethod
    async def query_from_json(record: Dict[list, dict]) -> str:
        query = ""

        constraints = await get_constraints()

        for tablename, column in record.items():

            table_query = f"CREATE TABLE {tablename} (\n\t"

            columns_query = ""
            column_len = len(column)

            _constraints = find_constraint(constraints, tablename)
            for item in _constraints:
                column_name = item['column_name']
                constraint_type = item['constraint_type']
                constraint_name = item['constraint_name']
                columns_query = "".join((columns_query, f'CONSTRAINT {constraint_name} {constraint_type} ({column_name})', ",\n\t"))

            for i, _column in enumerate(column.items()):
                column_name, column_value = _column
                data_type = column_value["data_type"]
                is_nullable = column_value["is_nullable"]
                char_len = column_value['char_len']
                column_default = column_value['column_default']

                columns_query = "".join((columns_query, column_name)).strip()
                columns_query = " ".join((columns_query, data_type, char_len, str(is_nullable), column_default)).strip()
                if i != column_len - 1:
                    columns_query = "".join((columns_query, ",\n\t"))



            query = "".join((query, table_query, columns_query, "\n);\n\n"))

        return query

    @staticmethod
    def create_query(version_id: str, query: str) -> None:
        with open(f"migrations/{version_id}.sql", "w+") as migration_file:
            migration_file.write(query)

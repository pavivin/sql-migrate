from typing import Dict


class MigrateQuery:
    @staticmethod
    def query_from_json(record: Dict[list, dict]) -> str:
        query = ""
        for tablename, column in record.items():

            table_query = f"CREATE TABLE {tablename} (\n\t"

            columns_query = ""
            column_len = len(column)

            for i, _column in enumerate(column.items()):
                column_name, column_value = _column
                data_type = column_value["data_type"]
                is_nullable = column_value["is_nullable"]

                columns_query = "".join((columns_query, column_name))
                columns_query = " ".join((columns_query, data_type, is_nullable))
                if i != column_len - 1:
                    columns_query = "".join((columns_query, ",\n\t"))

            query = "".join((query, table_query, columns_query, "\n);\n\n"))

        return query

    @staticmethod
    def create_query(version_id: str, query: str) -> None:
        with open(f"migrations/{version_id}.sql", "w+") as migration_file:
            migration_file.write(query)

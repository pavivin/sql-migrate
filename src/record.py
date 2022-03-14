from typing import Dict, Optional
import ujson
from pydantic import BaseModel, validator


class ValidatedRecord(BaseModel):
    table_name: str
    column_name: str
    data_type: str
    is_nullable: str
    character_maximum_length: Optional[str]
    column_default: Optional[str]

    @validator("column_default")
    def set_column_default(column_default):
        return f'DEFAULT {column_default}' if column_default else ""

    @validator("character_maximum_length")
    def set_char_len(char_len):
        return f'({char_len})' if char_len else ""

    @validator("is_nullable")
    def set_data_type(is_nullable):
        return "NULL" if is_nullable == "YES" else "NOT NULL"


class JsonRecord:
    @staticmethod
    def create_record(version_id: str, record: dict) -> None:
        with open(f"records/{version_id}.json", "w+") as record_file:
            record_file.write(ujson.dumps(record))

    @staticmethod
    def record_from_file(version_id):
        with open(f"records/{version_id}.json", "r") as record_file:
            return ujson.loads(record_file.read())

    @staticmethod
    def get_record_diff(cur_record: dict, last_record: dict) -> dict:
        diff = {}
        for table_name, table_value in last_record.items():
            diff[table_name] = diff.get(table_name, {})
            for column_name, column_value in table_value.items():
                if cur_record[table_name].get(column_name):
                    # TODO: get difference from alter table and drop table
                    column_diff = dict(
                        set(column_value) ^ set(cur_record[table_name][column_name])
                    )
                else:
                    diff[table_name][column_name] = column_value
                if column_diff:
                    diff[table_name][column_name] = column_diff

            if not diff[table_name]:
                del diff[table_name]
        return diff

    @staticmethod
    def aggregate_db_info(db_info: dict) -> Dict[list, dict]:
        record_json = {}

        for row in db_info:
            column = ValidatedRecord(**row)
            record_json[column.table_name] = record_json.get(column.table_name, {})
            record_json[column.table_name][column.column_name] = {
                "data_type": column.data_type,
                "is_nullable": column.is_nullable,
                "char_len": column.character_maximum_length,
                "column_default": column.column_default
            }

        return record_json

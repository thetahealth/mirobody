"""JSON-safe conversion of database rows.

The rest of this module (row_to_dict/rows_to_list, format_datetime,
get_pagination_params, parse_json_field, calculate_age, format_value_with_unit,
get_query_time) was removed: nothing in the repo referenced any of it, and
several were near-duplicates of helpers that do live elsewhere.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any


class DataConverter:
    """Make DB values JSON-serializable.

    psycopg hands back `datetime`/`date`/`Decimal` objects that `json.dumps`
    refuses. Tool results go out over JSON-RPC, so they have to be coerced
    before they reach the wire.
    """

    @staticmethod
    def convert_special_types(data: Any) -> Any:
        if isinstance(data, (datetime, date)):
            return data.isoformat()
        if isinstance(data, Decimal):
            return float(data)
        if isinstance(data, dict):
            return {k: DataConverter.convert_special_types(v) for k, v in data.items()}
        if isinstance(data, list):
            return [DataConverter.convert_special_types(item) for item in data]
        return data

    async def convert_list(self, data: list[Any]) -> list[Any]:
        return self.convert_special_types(data) if data else []

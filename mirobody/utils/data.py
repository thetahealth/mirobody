import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.engine import Row


def row_to_dict(row: Row) -> Dict[str, Any]:
    """Convert SQLAlchemy Row object to dictionary"""
    return {key: value for key, value in row._mapping.items()}


def rows_to_list(rows: List[Row]) -> List[Dict[str, Any]]:
    """Convert list of SQLAlchemy Row objects to list of dictionaries"""
    return [row_to_dict(row) for row in rows]


def format_datetime(dt: Union[datetime, str, None]) -> Optional[str]:
    """Format datetime object to string"""
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            return dt
    return dt.isoformat()


def get_pagination_params(page: int = 1, page_size: int = 20) -> Dict[str, int]:
    """Get pagination parameters"""
    offset = (page - 1) * page_size
    return {"limit": page_size, "offset": offset}


def parse_json_field(data: str) -> Dict[str, Any]:
    """Parse JSON field"""
    if not data:
        return {}
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return {}


def calculate_age(birth_date: str) -> Optional[int]:
    """Calculate age from birth date"""
    if not birth_date:
        return None

    try:
        birth_date_obj = datetime.strptime(birth_date, "%Y-%m-%d")
        today = datetime.now()
        age = today.year - birth_date_obj.year

        # Check if birthday has passed
        if (today.month, today.day) < (birth_date_obj.month, birth_date_obj.day):
            age -= 1

        return age
    except ValueError:
        return None


def format_value_with_unit(value: str, unit: str) -> str:
    """Format value with unit"""
    if not value:
        return ""
    if not unit:
        return value
    return f"{value} {unit}"


class DataConverter:
    """Data type conversion utility class"""

    @staticmethod
    def convert_special_types(data: Any) -> Any:
        """Recursively convert datetime, date and Decimal objects to serializable types"""
        if isinstance(data, (datetime, date)):
            return data.isoformat()
        elif isinstance(data, Decimal):
            return float(data)
        elif isinstance(data, dict):
            return {key: DataConverter.convert_special_types(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [DataConverter.convert_special_types(item) for item in data]
        else:
            return data

    async def convert_list(self, data: List[Any]) -> List[Any]:
        """Asynchronously convert special types in list data"""
        if not data:
            return []
        return self.convert_special_types(data)


def get_query_time(start_date: Optional[str], end_date: Optional[str]) -> tuple:
    """
    Process date parameters, set to last year if empty

    Args:
        start_date: Start date string in YYYY-MM-DD format
        end_date: End date string in YYYY-MM-DD format

    Returns:
        tuple: (start_datetime, end_datetime) datetime object tuple
    """
    today = datetime.now()
    if not end_date:
        end_date = today.strftime("%Y-%m-%d")
    if not start_date:
        one_year_ago = today - timedelta(days=365)
        start_date = one_year_ago.strftime("%Y-%m-%d")

    # Convert string dates to datetime objects
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

    # Add one day to end date to ensure current day data is included
    end_datetime = end_datetime + timedelta(days=1)

    return start_datetime, end_datetime

from typing import Any
from datetime import datetime
from dateutil.tz import tzlocal

def convert_datetime_fields(obj: Any) -> Any:
    """Convert any datetime or tzlocal objects to string in the given object"""
    if isinstance(obj, dict):
        return {k: convert_datetime_fields(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_fields(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, tzlocal):
        offset = datetime.now(tzlocal()).strftime('%z')
        return f"UTC{offset[:3]}:{offset[3:]}"
    return obj
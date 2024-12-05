import sqlite3
from enum import Enum
from typing import Any, Dict, List, Optional
import json

class AggregateFunction(str, Enum):
    """Supported aggregation functions."""
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"

class Aggregations:
    """Aggregation operations for collections."""
    
    def __init__(self, database):
        """Initialize with database connection."""
        self.database = database
    
    def execute_pipeline(self, collection: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute an aggregation pipeline."""
        with self.database.pool.get_connection() as conn:
            cursor = conn.cursor()
            
            for stage in pipeline:
                if "group" in stage:
                    group = stage["group"]
                    field = group.get("field")
                    func = group["function"].value
                    alias = group["alias"]
                    target = group.get("target", field)
                    
                    # Build SQL query
                    if field:
                        # Group by field
                        sql = f"""
                            SELECT json_extract(data, '$.{field}') as group_field,
                                   {func}(CAST(json_extract(data, '$.{target}') AS NUMERIC)) as {alias}
                            FROM documents
                            WHERE collection = ?
                            GROUP BY json_extract(data, '$.{field}')
                        """
                    else:
                        # Global aggregation
                        sql = f"""
                            SELECT {func}(CAST(json_extract(data, '$.{target}') AS NUMERIC)) as {alias}
                            FROM documents
                            WHERE collection = ?
                        """
                    
                    cursor.execute(sql, [collection])
                    
                    # Process results
                    results = []
                    for row in cursor:
                        if field:
                            try:
                                field_value = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                            except (json.JSONDecodeError, TypeError):
                                field_value = row[0]
                            results.append({
                                field: field_value,
                                alias: row[1]
                            })
                        else:
                            results.append({alias: row[0]})
                    
                    return results
            
            return []
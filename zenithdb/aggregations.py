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
    MEDIAN = "MEDIAN"  # Custom implementation
    STDDEV = "STDDEV"  # Standard deviation
    COUNT_DISTINCT = "COUNT_DISTINCT"  # Count distinct values

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
                    
                    # Build SQL query based on function
                    if func == AggregateFunction.MEDIAN.value:
                        # SQLite doesn't have a MEDIAN function, so we have to implement it
                        if field:
                            # Group by field with median
                            sql = f"""
                                SELECT json_extract(data, '$.{field}') as group_field,
                                       (SELECT json_extract(d.data, '$.{target}') 
                                        FROM documents d
                                        WHERE d.collection = ? AND json_extract(d.data, '$.{field}') = json_extract(documents.data, '$.{field}')
                                        ORDER BY CAST(json_extract(d.data, '$.{target}') AS NUMERIC)
                                        LIMIT 1
                                        OFFSET (SELECT COUNT(*) 
                                                FROM documents d2 
                                                WHERE d2.collection = ? AND json_extract(d2.data, '$.{field}') = json_extract(documents.data, '$.{field}')
                                               ) / 2) as {alias}
                                FROM documents
                                WHERE collection = ?
                                GROUP BY json_extract(data, '$.{field}')
                            """
                            params = [collection, collection, collection]
                        else:
                            # Global median
                            sql = f"""
                                SELECT (SELECT json_extract(data, '$.{target}') 
                                        FROM documents 
                                        WHERE collection = ?
                                        ORDER BY CAST(json_extract(data, '$.{target}') AS NUMERIC)
                                        LIMIT 1
                                        OFFSET (SELECT COUNT(*) FROM documents WHERE collection = ?) / 2) as {alias}
                            """
                            params = [collection, collection]
                    elif func == AggregateFunction.STDDEV.value:
                        # SQLite doesn't have a built-in STDDEV function
                        if field:
                            # Group by field with standard deviation
                            sql = f"""
                                SELECT json_extract(data, '$.{field}') as group_field,
                                       SQRT(AVG(CAST(json_extract(data, '$.{target}') AS NUMERIC) * 
                                                CAST(json_extract(data, '$.{target}') AS NUMERIC)) - 
                                            (AVG(CAST(json_extract(data, '$.{target}') AS NUMERIC)) * 
                                             AVG(CAST(json_extract(data, '$.{target}') AS NUMERIC)))) as {alias}
                                FROM documents
                                WHERE collection = ?
                                GROUP BY json_extract(data, '$.{field}')
                            """
                            params = [collection]
                        else:
                            # Global standard deviation
                            sql = f"""
                                SELECT SQRT(AVG(CAST(json_extract(data, '$.{target}') AS NUMERIC) * 
                                            CAST(json_extract(data, '$.{target}') AS NUMERIC)) - 
                                        (AVG(CAST(json_extract(data, '$.{target}') AS NUMERIC)) * 
                                         AVG(CAST(json_extract(data, '$.{target}') AS NUMERIC)))) as {alias}
                                FROM documents
                                WHERE collection = ?
                            """
                            params = [collection]
                    elif func == AggregateFunction.COUNT_DISTINCT.value:
                        if field:
                            # Group by field with distinct count
                            sql = f"""
                                SELECT json_extract(data, '$.{field}') as group_field,
                                       COUNT(DISTINCT json_extract(data, '$.{target}')) as {alias}
                                FROM documents
                                WHERE collection = ?
                                GROUP BY json_extract(data, '$.{field}')
                            """
                            params = [collection]
                        else:
                            # Global distinct count
                            sql = f"""
                                SELECT COUNT(DISTINCT json_extract(data, '$.{target}')) as {alias}
                                FROM documents
                                WHERE collection = ?
                            """
                            params = [collection]
                    else:
                        # Regular aggregation functions
                        if field:
                            # Group by field
                            sql = f"""
                                SELECT json_extract(data, '$.{field}') as group_field,
                                       {func}(CAST(json_extract(data, '$.{target}') AS NUMERIC)) as {alias}
                                FROM documents
                                WHERE collection = ?
                                GROUP BY json_extract(data, '$.{field}')
                            """
                            params = [collection]
                        else:
                            # Global aggregation
                            sql = f"""
                                SELECT {func}(CAST(json_extract(data, '$.{target}') AS NUMERIC)) as {alias}
                                FROM documents
                                WHERE collection = ?
                            """
                            params = [collection]
                    
                    cursor.execute(sql, params)
                    
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
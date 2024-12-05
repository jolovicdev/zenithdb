from enum import Enum
from typing import Any, List, Tuple, Optional, Dict

class QueryOperator(str, Enum):
    """Query operators."""
    EQ = "="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    IN = "IN"
    BETWEEN = "BETWEEN"
    CONTAINS = "CONTAINS"

class QueryField:
    """Represents a field in a query with operator overloading."""
    
    def __init__(self, field_path: str, query):
        self.field_path = field_path
        self.query = query
    
    def __eq__(self, other: Any) -> 'Query':
        """Equal to operator."""
        self.query.where(self.field_path, QueryOperator.EQ, other)
        return self.query
    
    def __ne__(self, other: Any) -> 'Query':
        """Not equal to operator."""
        self.query.where(self.field_path, QueryOperator.NE, other)
        return self.query
    
    def __gt__(self, other: Any) -> 'Query':
        """Greater than operator."""
        self.query.where(self.field_path, QueryOperator.GT, other)
        return self.query
    
    def __ge__(self, other: Any) -> 'Query':
        """Greater than or equal operator."""
        self.query.where(self.field_path, QueryOperator.GTE, other)
        return self.query
    
    def __lt__(self, other: Any) -> 'Query':
        """Less than operator."""
        self.query.where(self.field_path, QueryOperator.LT, other)
        return self.query
    
    def __le__(self, other: Any) -> 'Query':
        """Less than or equal operator."""
        self.query.where(self.field_path, QueryOperator.LTE, other)
        return self.query
    
    def contains(self, value: Any) -> 'Query':
        """Check if array field contains value."""
        self.query.where(self.field_path, QueryOperator.CONTAINS, value)
        return self.query
    
    def between(self, start: Any, end: Any) -> 'Query':
        """Check if field is between start and end values."""
        self.query.where(self.field_path, QueryOperator.BETWEEN, [start, end])
        return self.query
    
    def __getattr__(self, name: str) -> 'QueryField':
        """Support for nested fields."""
        return QueryField(f"{self.field_path}.{name}", self.query)

class Query:
    """Query builder with operator overloading support."""
    
    def __init__(self, collection: Optional[str] = None, database = None):
        """Initialize a new query."""
        self.conditions: List[Tuple[str, QueryOperator, Any]] = []
        self.collection = collection
        self.database = database
        self.sort_fields: List[Tuple[str, str]] = []
        self.limit_value: Optional[int] = None
        self.skip_value: Optional[int] = None
        self._query_cache = {}  # Add query cache
    
    def __getattr__(self, name: str) -> QueryField:
        """Support for field access."""
        return QueryField(name, self)
    
    def where(self, field: str, operator: QueryOperator, value: Any) -> 'Query':
        """Add a where condition."""
        self.conditions.append((field, operator, value))
        return self
    
    def sort(self, field: str, ascending: bool = True) -> 'Query':
        """Add sort criteria."""
        self.sort_fields.append((field, "ASC" if ascending else "DESC"))
        return self
    
    def limit(self, value: int) -> 'Query':
        """Set limit value."""
        self.limit_value = value
        return self
    
    def skip(self, value: int) -> 'Query':
        """Set skip value."""
        self.skip_value = value
        return self
    
    def __and__(self, other: 'Query') -> 'Query':
        """Combine two queries with AND."""
        if isinstance(other, Query):
            new_query = Query(self.collection, self.database)
            new_query.conditions = self.conditions + other.conditions
            new_query.sort_fields = self.sort_fields
            new_query.limit_value = self.limit_value
            new_query.skip_value = self.skip_value
            return new_query
        return self
    
    def execute(self) -> List[Dict[str, Any]]:
        """Execute the query with caching."""
        cache_key = self._generate_cache_key()
        if cache_key in self._query_cache:
            return self._query_cache[cache_key]
            
        result = self.database.execute_query(self)
        self._query_cache[cache_key] = result
        return result
    
    def _generate_cache_key(self) -> str:
        """Generate a unique cache key for the query."""
        return hash(str(self.to_dict()))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert query to dictionary format."""
        result = {}
        for field, op, value in self.conditions:
            # For nested fields, we want to keep them flat in the query
            # but with dot notation
            if op == QueryOperator.EQ:
                result[field] = value
            elif op == QueryOperator.CONTAINS:
                if field not in result:
                    result[field] = {}
                result[field]["$contains"] = value
            elif op == QueryOperator.IN:
                if field not in result:
                    result[field] = {}
                result[field]["$in"] = value
            else:
                op_map = {
                    QueryOperator.GT: "$gt",
                    QueryOperator.GTE: "$gte",
                    QueryOperator.LT: "$lt",
                    QueryOperator.LTE: "$lte",
                    QueryOperator.NE: "$ne",
                    QueryOperator.BETWEEN: "$between"
                }
                if field not in result:
                    result[field] = {}
                result[field][op_map[op]] = value
        return result
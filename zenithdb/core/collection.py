from typing import Any, Dict, List, Optional, Union
from ..query import Query, QueryOperator
from ..operations import BulkOperations
from ..aggregations import Aggregations, AggregateFunction

class Collection:
    """Collection interface for document operations."""
    
    def __init__(self, database, name: str):
        """Initialize collection with database connection and name."""
        self.database = database
        self.name = name
    
    def insert(self, document: Dict[str, Any], doc_id: Optional[str] = None) -> str:
        """Insert a document into the collection."""
        return self.database.insert(self.name, document, doc_id)
    
    def insert_many(self, documents: List[Dict[str, Any]]) -> List[str]:
        """Insert multiple documents into the collection."""
        with self.database.bulk_operations() as bulk_ops:
            return bulk_ops.bulk_insert(self.name, documents)
    
    def find(self, query: Optional[Union[Dict[str, Any], Query]] = None) -> List[Dict[str, Any]]:
        """Find documents in the collection."""
        if query is None:
            query = Query(self.name, self.database)
            return query.execute()
            
        if isinstance(query, dict):
            base_query = Query(self.name, self.database)
            for field, value in query.items():
                if isinstance(value, dict):
                    for op, val in value.items():
                        op = op.lstrip("$")  # Remove $ prefix
                        if op == "gte":
                            base_query.where(field, QueryOperator.GTE, val)
                        elif op == "lte":
                            base_query.where(field, QueryOperator.LTE, val)
                        elif op == "in":
                            base_query.where(field, QueryOperator.IN, val)
                        elif op == "contains":
                            base_query.where(field, QueryOperator.CONTAINS, val)
                        else:
                            base_query.where(field, QueryOperator.EQ, val)
                else:
                    base_query.where(field, QueryOperator.EQ, value)
            return base_query.execute()
        
        # If it's already a Query object
        query.collection = self.name
        query.database = self.database
        return query.execute()
    
    def find_one(self, query: Optional[Union[Dict[str, Any], Query]] = None) -> Optional[Dict[str, Any]]:
        """Find a single document in the collection."""
        if query is None:
            query = Query(self.name, self.database)
            query.limit(1)
            results = query.execute()
            return results[0] if results else None
        
        if isinstance(query, dict):
            base_query = Query(self.name, self.database)
            for field, value in query.items():
                if isinstance(value, dict):
                    for op, val in value.items():
                        op = op.lstrip("$")
                        if op == "gte":
                            base_query.where(field, QueryOperator.GTE, val)
                        elif op == "lte":
                            base_query.where(field, QueryOperator.LTE, val)
                        elif op == "in":
                            base_query.where(field, QueryOperator.IN, val)
                        elif op == "contains":
                            base_query.where(field, QueryOperator.CONTAINS, val)
                        else:
                            base_query.where(field, QueryOperator.EQ, val)
                else:
                    base_query.where(field, QueryOperator.EQ, value)
            base_query.limit(1)
            results = base_query.execute()
            return results[0] if results else None
        
        # If it's already a Query object
        query.collection = self.name
        query.database = self.database
        query.limit(1)
        results = query.execute()
        return results[0] if results else None
    
    def update(self, query: Union[Dict[str, Any], Query], update: Dict[str, Any]) -> int:
        """Update documents in the collection."""
        if isinstance(query, Query):
            query_dict = query.to_dict()
        else:
            query_dict = query

        # Wrap update in $set if not already wrapped
        if not any(key.startswith('$') for key in update.keys()):
            update = {'$set': update}

        return self.database.update(self.name, query_dict, update)
    
    def delete(self, query: Union[Dict[str, Any], Query]) -> int:
        """Delete documents matching query."""
        if isinstance(query, Query):
            query_dict = query.to_dict()
        else:
            query_dict = query

        return self.database.delete(self.name, query_dict)
    
    def delete_many(self, query: Optional[Union[Dict[str, Any], Query]] = None) -> int:
        """Delete multiple documents from the collection."""
        if query is None:
            query = {}
        return self.delete(query)
    
    def aggregate(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute an aggregation pipeline."""
        agg = Aggregations(self.database)
        return agg.execute_pipeline(self.name, pipeline)
    
    def bulk_operations(self) -> BulkOperations:
        """Get bulk operations interface."""
        return self.database.bulk_operations()
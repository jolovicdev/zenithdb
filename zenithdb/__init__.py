"""
ZenithDB - A high-performance document database built on SQLite
"""

from .core.database import Database
from .core.collection import Collection
from .query import Query, QueryOperator
from .operations import BulkOperations
from .aggregations import Aggregations, AggregateFunction

__version__ = "0.1.0"
__all__ = [
    'Database',
    'Collection',
    'Query',
    'QueryOperator',
    'BulkOperations',
    'Aggregations',
    'AggregateFunction'
]

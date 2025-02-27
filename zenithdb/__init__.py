"""
ZenithDB - A high-performance document database built on SQLite

ZenithDB provides MongoDB-like syntax for working with SQLite, enabling
document storage, advanced querying, full-text search, and more while
leveraging SQLite's reliability and performance.
"""

from .core.database import Database
from .core.collection import Collection
from .query import Query, QueryOperator
from .operations import BulkOperations
from .aggregations import Aggregations, AggregateFunction

__version__ = "2.0.0"
__all__ = [
    'Database',
    'Collection',
    'Query',
    'QueryOperator',
    'BulkOperations',
    'Aggregations',
    'AggregateFunction'
]

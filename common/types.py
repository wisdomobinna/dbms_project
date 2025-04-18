"""
Common data types used throughout the DBMS.
"""

from enum import Enum, auto

class DataType(Enum):
    """Enumeration of supported data types."""
    INTEGER = auto()
    STRING = auto()

class ColumnConstraint(Enum):
    """Enumeration of supported column constraints."""
    PRIMARY_KEY = auto()
    FOREIGN_KEY = auto()
    NOT_NULL = auto()
    UNIQUE = auto()
    AUTO_INCREMENT = auto()
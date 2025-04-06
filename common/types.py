"""
Common data types used throughout the DBMS.
"""

from enum import Enum, auto

class DataType(Enum):
    """Enumeration of supported data types."""
    INTEGER = auto()
    STRING = auto()
"""
Custom exceptions for the DBMS.
"""

class DBMSError(Exception):
    """Base class for all DBMS exceptions."""
    pass

class ParseError(DBMSError):
    """Exception raised for errors during SQL parsing."""
    pass

class ValidationError(DBMSError):
    """Exception raised for SQL validation errors."""
    pass

class SchemaError(DBMSError):
    """Exception raised for schema-related errors."""
    pass

class StorageError(DBMSError):
    """Exception raised for storage errors."""
    pass

class IndexError(DBMSError):
    """Exception raised for index-related errors."""
    pass

class ExecutionError(DBMSError):
    """Exception raised for query execution errors."""
    pass
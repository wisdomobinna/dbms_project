# parser/__init__.py
"""
SQL parsing module using PLY.
"""

# Fix imports to use relative paths
from .sql_parser import SQLParser
from .sql_grammar import *  # Import grammar rules
from .sql_lexer import SQLLexer  # Import lexer

__all__ = ['SQLParser']
#!/usr/bin/env python3
"""
Test script to verify database queries work
"""

import os
import sys
import time
import argparse
import readline  # For command history and editing capabilities
from colorama import init, Fore, Style  # For colored output

# Fix imports to use relative imports without package
# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parser.sql_parser import SQLParser
from catalog.schema_manager import SchemaManager
from storage.disk_manager import DiskManager
from storage.index.index_manager import IndexManager
from query.optimizer import QueryOptimizer
from execution.executor import Executor
from common.types import DataType

# Initialize colorama
init()

# Create a test parser instance
parser = SQLParser()

# Test some queries
test_queries = [
    "INSERT INTO students VALUES (1, 'Sean Cross', 18);",
    "SELECT * FROM students;",
    "INSERT INTO students VALUES (2, 'Jane Smith', 20);",
    "SELECT * FROM students WHERE age > 18;"
]

print(f"{Fore.CYAN}Testing SQL parsing...{Style.RESET_ALL}")
for query in test_queries:
    print(f"\n{Fore.YELLOW}Query:{Style.RESET_ALL} {query}")
    try:
        # Remove trailing semicolon for parsing
        clean_query = query.strip()
        if clean_query.endswith(';'):
            clean_query = clean_query[:-1]
            
        result = parser.parse(clean_query)
        print(f"{Fore.GREEN}Successfully parsed!{Style.RESET_ALL}")
        print(f"Parsed result type: {result.get('type', 'unknown')}")
    except Exception as e:
        print(f"{Fore.RED}Error parsing: {str(e)}{Style.RESET_ALL}")
        
print(f"\n{Fore.CYAN}SQL parsing test complete.{Style.RESET_ALL}")
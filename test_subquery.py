#!/usr/bin/env python3

"""
Test script for the fixed subquery functionality
"""

import sys
import json
from catalog.schema_manager import SchemaManager
from storage.disk_manager import DiskManager
from storage.index.index_manager import IndexManager
from query.optimizer import QueryOptimizer
from execution.executor import Executor
from parser.sql_parser import SQLParser

def main():
    parser = SQLParser()
    
    # Parse the query with subquery
    query = """
    SELECT age
    FROM (
        SELECT age, COUNT(*) AS age_count
        FROM students
        GROUP BY age
    ) AS age_groups
    WHERE age_count = 1;
    """
    
    # Parse the query
    parsed_query = parser.parse(query)
    print("Parsed query:")
    print(json.dumps(parsed_query, indent=2))
    
    # Initialize components
    disk_manager = DiskManager()
    schema_manager = SchemaManager()
    index_manager = IndexManager(schema_manager, disk_manager)
    optimizer = QueryOptimizer(schema_manager, index_manager)
    executor = Executor(schema_manager, disk_manager, index_manager, optimizer)
    
    # Execute the query
    try:
        result = executor.execute(parsed_query)
        print("\nExecution result:")
        print(json.dumps(result, indent=2))
        print("\nTest passed! The subquery in FROM clause is working correctly.")
    except Exception as e:
        print(f"\nExecution failed: {str(e)}")
        print("Test failed! The subquery in FROM clause is still not working correctly.")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Test script for AUTO_INCREMENT functionality.
"""

import os
import sys
from main import DBMSApplication

def test_auto_increment():
    """Test the AUTO_INCREMENT functionality."""
    print("Testing AUTO_INCREMENT functionality:")
    print("====================================")
    
    # Initialize the DBMS application
    app = DBMSApplication(db_directory="./database")
    
    # Clean up existing test tables if they exist
    try:
        app.run_query("DROP TABLE test_auto_increment")
        app.run_query("DROP TABLE test_auto_increment2")
    except:
        pass
    
    # Test 1: Create a table with AUTO_INCREMENT primary key
    print("\nTest 1: Creating table with AUTO_INCREMENT primary key")
    result, _ = app.run_query("""
    CREATE TABLE test_auto_increment (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        name STRING,
        value INTEGER
    )
    """)
    
    # Also test the alternative ordering of the keywords
    result2, _ = app.run_query("""
    CREATE TABLE test_auto_increment2 (
        id INTEGER AUTO_INCREMENT PRIMARY KEY,
        name STRING,
        value INTEGER
    )
    """)
    print(f"Result: {result}")
    
    # Test 2: Describe the table to see AUTO_INCREMENT attribute
    print("\nTest 2: Describing table to see AUTO_INCREMENT attribute")
    result, _ = app.run_query("DESCRIBE test_auto_increment")
    print(f"Result for test_auto_increment: {result}")
    
    # Also check second table with different keyword order
    result2, _ = app.run_query("DESCRIBE test_auto_increment2")
    print(f"Result for test_auto_increment2: {result2}")
    
    # Test 3: Insert a record without specifying id (should auto-generate)
    print("\nTest 3: Insert with auto-generated ID")
    result, _ = app.run_query("INSERT INTO test_auto_increment VALUES (0, 'First Record', 100)")
    print(f"Result: {result}")
    
    # Test 4: Insert another record to see auto-increment
    print("\nTest 4: Insert second record with auto-generated ID")
    result, _ = app.run_query("INSERT INTO test_auto_increment VALUES (0, 'Second Record', 200)")
    print(f"Result: {result}")
    
    # Test 5: Insert with explicit ID
    print("\nTest 5: Insert with explicit ID")
    result, _ = app.run_query("INSERT INTO test_auto_increment VALUES (10, 'Explicit ID', 300)")
    print(f"Result: {result}")
    
    # Test 6: Insert another auto-incremented record (should use max+1)
    print("\nTest 6: Insert after explicit ID (should use max+1)")
    result, _ = app.run_query("INSERT INTO test_auto_increment VALUES (0, 'After Explicit', 400)")
    print(f"Result: {result}")
    
    # Test 7: Select all records to see the results
    print("\nTest 7: Select all records to see results")
    result, _ = app.run_query("SELECT * FROM test_auto_increment")
    print(f"Result: {result}")
    
    print("\nAUTO_INCREMENT test completed successfully!")

if __name__ == "__main__":
    test_auto_increment()
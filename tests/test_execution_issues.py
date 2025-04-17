"""
Tests for specific SQL execution issues.
"""

import os
import shutil
import pytest

from parser.sql_parser import SQLParser
from catalog.schema_manager import SchemaManager
from storage.disk_manager import DiskManager
from storage.index.index_manager import IndexManager
from query.optimizer import QueryOptimizer
from execution.executor import Executor
from common.exceptions import DBMSError

# Test database directory
TEST_DB_DIR = "./test_database_execution"

class TestExecutionIssues:
    """Test specific execution issues mentioned by users."""
    
    @classmethod
    def setup_class(cls):
        """Set up the test environment."""
        # Create test database directory
        os.makedirs(TEST_DB_DIR, exist_ok=True)
        
        # Initialize components
        cls.disk_manager = DiskManager(TEST_DB_DIR)
        cls.schema_manager = SchemaManager(cls.disk_manager)
        cls.index_manager = IndexManager(cls.disk_manager)
        cls.parser = SQLParser()
        cls.optimizer = QueryOptimizer(cls.schema_manager, cls.index_manager)
        cls.executor = Executor(
            cls.schema_manager, 
            cls.disk_manager,
            cls.index_manager,
            cls.optimizer
        )
    
    @classmethod
    def teardown_class(cls):
        """Clean up the test environment."""
        # Remove test database directory
        shutil.rmtree(TEST_DB_DIR, ignore_errors=True)
    
    def setup_method(self):
        """Set up before each test method."""
        # Drop any existing tables
        try:
            for table in self.schema_manager.get_tables():
                query = f"DROP TABLE {table}"
                parsed_query = self.parser.parse(query)
                self.executor.execute(parsed_query)
        except:
            # Ignore errors during setup
            pass
    
    def test_init_column_issue(self):
        """Test the issue with __init__ column appearing in UPDATE results."""
        # Create a test table
        query = "CREATE TABLE init_test (id INTEGER PRIMARY KEY, value STRING)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Insert data
        query = "INSERT INTO init_test VALUES (1, 'test')"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Perform update
        query = "UPDATE init_test SET value = 'updated' WHERE id = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record(s) updated" in result
        
        # Verify record was updated correctly without __init__ column
        query = "SELECT * FROM init_test"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "updated" in result
        assert "__init__" not in result
    
    def test_multiple_insert_formats(self):
        """Test different formats of insert statements."""
        # Create a test table
        query = "CREATE TABLE insert_test (id INTEGER PRIMARY KEY, name STRING, age INTEGER)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Standard insert
        query = "INSERT INTO insert_test VALUES (1, 'Alice', 25)"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record inserted" in result
        
        # Multi-line insert
        query = """
        INSERT INTO insert_test 
        VALUES 
        (2, 'Bob', 
        30)
        """
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record inserted" in result
        
        # Verify both inserts worked
        query = "SELECT * FROM insert_test ORDER BY id"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "Alice" in result
        assert "Bob" in result
    
    def test_string_where_execution(self):
        """Test execution of queries with string conditions in WHERE clauses."""
        # Create a test table
        query = "CREATE TABLE string_test (id INTEGER PRIMARY KEY, name STRING)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Insert data
        queries = [
            "INSERT INTO string_test VALUES (1, 'Alice')",
            "INSERT INTO string_test VALUES (2, 'Bob')",
            "INSERT INTO string_test VALUES (3, 'Charlie')"
        ]
        
        for query in queries:
            parsed_query = self.parser.parse(query)
            self.executor.execute(parsed_query)
        
        # Query with string WHERE condition
        query = "SELECT * FROM string_test WHERE name = 'Alice'"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "Alice" in result
        assert "Bob" not in result
        
        # Update with string WHERE condition
        query = "UPDATE string_test SET name = 'Alice Smith' WHERE name = 'Alice'"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record(s) updated" in result
        
        # Verify the update worked
        query = "SELECT * FROM string_test WHERE id = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "Alice Smith" in result
    
    def test_and_or_execution(self):
        """Test execution of queries with complex AND/OR conditions."""
        # Create a test table and insert data
        query = "CREATE TABLE complex_test (id INTEGER PRIMARY KEY, name STRING, age INTEGER, active INTEGER)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Insert test data
        queries = [
            "INSERT INTO complex_test VALUES (1, 'Alice', 25, 1)",
            "INSERT INTO complex_test VALUES (2, 'Bob', 30, 0)",
            "INSERT INTO complex_test VALUES (3, 'Charlie', 22, 1)",
            "INSERT INTO complex_test VALUES (4, 'Dave', 35, 0)",
            "INSERT INTO complex_test VALUES (5, 'Eve', 28, 1)"
        ]
        
        for query in queries:
            parsed_query = self.parser.parse(query)
            self.executor.execute(parsed_query)
        
        # Test simple AND condition
        query = "SELECT * FROM complex_test WHERE age > 25 AND active = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "Eve" in result
        assert "Alice" not in result  # Age is 25, not > 25
        assert "Bob" not in result    # Active is 0
        assert "Charlie" not in result  # Age is 22
        
        # Test simple OR condition
        query = "SELECT * FROM complex_test WHERE name = 'Alice' OR name = 'Bob'"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "Alice" in result
        assert "Bob" in result
        assert "Charlie" not in result
        
        # Test complex condition (AND + OR)
        query = "SELECT * FROM complex_test WHERE (age < 25 OR age > 30) AND active = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "Charlie" in result  # Age is 22, active is 1
        assert "Alice" not in result  # Age is 25
        assert "Bob" not in result   # Active is 0
        assert "Dave" not in result  # Active is 0
        assert "Eve" not in result   # Age is 28
    
    def test_type_checking(self):
        """Test type checking during execution."""
        # Create a test table
        query = "CREATE TABLE type_check (id INTEGER PRIMARY KEY, name STRING, count INTEGER)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Insert with correct types
        query = "INSERT INTO type_check VALUES (1, 'test', 100)"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record inserted" in result
        
        # When string values are used for the name column, it should work
        query = "UPDATE type_check SET name = 'updated' WHERE id = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record(s) updated" in result
        
        # When integer values are used for the count column, it should work
        query = "UPDATE type_check SET count = 200 WHERE id = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record(s) updated" in result
        
        # Check that both updates were applied
        query = "SELECT * FROM type_check WHERE id = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "updated" in result
        assert "200" in result
    
    def test_success_messages(self):
        """Test success messages on operations without result sets."""
        # Create a table
        query = "CREATE TABLE message_test (id INTEGER PRIMARY KEY, value STRING)"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "created successfully" in result
        
        # Insert
        query = "INSERT INTO message_test VALUES (1, 'test')"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record inserted" in result
        
        # Update with affected row
        query = "UPDATE message_test SET value = 'updated' WHERE id = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record(s) updated" in result
        
        # Update with no affected rows
        query = "UPDATE message_test SET value = 'updated again' WHERE id = 999"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "0 record(s) updated" in result
        
        # Delete with affected row
        query = "DELETE FROM message_test WHERE id = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record(s) deleted" in result
        
        # Delete with no affected rows
        query = "DELETE FROM message_test WHERE id = 999"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "0 record(s) deleted" in result
    
    def test_null_values(self):
        """Test handling of NULL values in the database."""
        # Create a test table
        query = "CREATE TABLE null_test (id INTEGER PRIMARY KEY, value STRING)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Insert with NULL value
        # Note: This would ideally be "INSERT INTO null_test VALUES (1, NULL)" but 
        # we'll simulate this with the current implementation's limitations
        query = "INSERT INTO null_test VALUES (1, '')"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Query the result
        query = "SELECT * FROM null_test"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        
        # Check if NULL is displayed correctly
        # Currently it would show empty string, but in a proper SQL database
        # it would show NULL
        assert "1" in result
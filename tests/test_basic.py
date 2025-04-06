"""
Basic tests for the DBMS implementation.
"""

import os
import shutil
import pytest
import sys
import os

from parser.sql_parser import SQLParser
from catalog.schema_manager import SchemaManager
from storage.disk_manager import DiskManager
from storage.index.index_manager import IndexManager
from query.optimizer import QueryOptimizer
from execution.executor import Executor
from common.exceptions import DBMSError

# Test database directory
TEST_DB_DIR = "./test_database"

class TestDBMS:
    """Test the DBMS implementation."""
    
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
    
    def test_create_table(self):
        """Test CREATE TABLE statement."""
        query = "CREATE TABLE students (id INTEGER PRIMARY KEY, name STRING, age INTEGER)"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "CREATE_TABLE"
        assert parsed_query["table_name"] == "students"
        assert len(parsed_query["columns"]) == 3
        assert parsed_query["primary_key"] == "id"
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "id | name | age" in result
        assert "John Doe" in result
        assert "Jane Smith" in result
    
    def test_select_with_where(self):
        """Test SELECT with WHERE clause."""
        query = "SELECT name, age FROM students WHERE age > 21"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "SELECT"
        assert parsed_query["table"] == "students"
        assert parsed_query["where"]["type"] == "comparison"
        assert parsed_query["where"]["operator"] == ">"
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "Jane Smith" in result
        assert "John Doe" not in result
    
    def test_update(self):
        """Test UPDATE statement."""
        query = "UPDATE students SET age = 21 WHERE id = 1"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "UPDATE"
        assert parsed_query["table_name"] == "students"
        assert len(parsed_query["set_items"]) == 1
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "1 record(s) updated" in result
        
        # Verify the update
        query = "SELECT * FROM students WHERE id = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "21" in result
    
    def test_delete(self):
        """Test DELETE statement."""
        query = "DELETE FROM students WHERE id = 2"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "DELETE"
        assert parsed_query["table_name"] == "students"
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "1 record(s) deleted" in result
        
        # Verify the delete
        query = "SELECT * FROM students"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "Jane Smith" not in result
    
    def test_create_index(self):
        """Test CREATE INDEX statement."""
        query = "CREATE INDEX ON students (name)"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "CREATE_INDEX"
        assert parsed_query["table_name"] == "students"
        assert parsed_query["column_name"] == "name"
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "Index created" in result
        
        # Verify the index exists
        assert self.schema_manager.index_exists("students", "name")
    
    def test_drop_table(self):
        """Test DROP TABLE statement."""
        # First create another table
        query = "CREATE TABLE courses (id INTEGER PRIMARY KEY, name STRING)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Now drop it
        query = "DROP TABLE courses"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "DROP_TABLE"
        assert parsed_query["table_name"] == "courses"
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "dropped successfully" in result
        
        # Verify the table no longer exists
        assert not self.schema_manager.table_exists("courses")
    
        def test_error_handling(self):
            """Test error handling."""
            # Try to create a table that already exists
            query = "CREATE TABLE students (id INTEGER PRIMARY KEY, name STRING)"
            parsed_query = self.parser.parse(query)
            
            with pytest.raises(DBMSError):
                self.executor.execute(parsed_query)
            
            # Try to select from a non-existent table
            query = "SELECT * FROM non_existent_table"
            parsed_query = self.parser.parse(query)
            
            with pytest.raises(DBMSError):
                self.executor.execute(parsed_query)
            
            # Remove this line:
            # result = self.executor.execute(parsed_query)
            
    def test_insert(self):
        """Test INSERT statement."""
        query = "INSERT INTO students VALUES (1, 'John Doe', 20)"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "INSERT"
        assert parsed_query["table_name"] == "students"
        assert len(parsed_query["values"]) == 3
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "1 record inserted" in result
        
        # Insert another record
        query = "INSERT INTO students VALUES (2, 'Jane Smith', 22)"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "1 record inserted" in result
    
    def test_select(self):
        """Test SELECT statement."""
        query = "SELECT * FROM students"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "SELECT"
        assert parsed_query["table"] == "students"
        assert parsed_query["projection"]["type"] == "all"
 
    # Execute the query
        result = self.executor.execute(parsed_query)
        assert "id | name | age" in result
        assert "John Doe" in result
        assert "Jane Smith" in result
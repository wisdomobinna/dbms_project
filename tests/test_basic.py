"""
Enhanced tests for the DBMS implementation.
"""

import os
import shutil
import pytest
import sys

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
        assert "created successfully" in result
    
    def test_insert(self):
        """Test INSERT statement."""
        # First create a table
        self._create_students_table()
        
        query = "INSERT INTO students VALUES (1, 'John Doe', 20)"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "INSERT"
        assert parsed_query["table_name"] == "students"
        assert len(parsed_query["values"]) == 3
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "record inserted" in result
        
        # Insert another record
        query = "INSERT INTO students VALUES (2, 'Jane Smith', 22)"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "record inserted" in result
    
    def test_select(self):
        """Test basic SELECT statement."""
        # Set up test data
        self._create_students_table()
        self._insert_sample_students()
        
        query = "SELECT * FROM students"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "SELECT"
        assert parsed_query["table"] == "students"
        assert parsed_query["projection"]["type"] == "all"
 
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "John Doe" in result
        assert "Jane Smith" in result
    
    def test_select_with_where(self):
        """Test SELECT with WHERE clause."""
        # Set up test data
        self._create_students_table()
        self._insert_sample_students()
        
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
    
    def test_select_with_order_by(self):
        """Test SELECT with ORDER BY clause."""
        # Set up test data
        self._create_students_table()
        self._insert_sample_students()
        
        # Add more students
        query = "INSERT INTO students VALUES (3, 'Alice Johnson', 19)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        query = "SELECT * FROM students ORDER BY age DESC"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "SELECT"
        assert parsed_query["order_by"] is not None
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        
        # The result should list Jane first (age 22), then John (age 20), then Alice (age 19)
        # Check if the order is correct by finding positions in the result string
        assert result.find("Jane Smith") < result.find("John Doe")
        assert result.find("John Doe") < result.find("Alice Johnson")
    
    def test_update(self):
        """Test UPDATE statement."""
        # Set up test data
        self._create_students_table()
        self._insert_sample_students()
        
        query = "UPDATE students SET age = 21 WHERE id = 1"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "UPDATE"
        assert parsed_query["table_name"] == "students"
        assert len(parsed_query["set_items"]) == 1
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        # Use a more flexible assertion that matches the actual format
        assert "record(s) updated in 'students'" in result
        
        # Verify the update
        query = "SELECT * FROM students WHERE id = 1"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "21" in result
    
    def test_delete(self):
        """Test DELETE statement."""
        # Set up test data
        self._create_students_table()
        self._insert_sample_students()
        
        query = "DELETE FROM students WHERE id = 2"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "DELETE"
        assert parsed_query["table_name"] == "students"
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        # Use a more flexible assertion for the message format
        assert "record(s) deleted from 'students'" in result
        
        # Verify the delete
        query = "SELECT * FROM students"
        parsed_query = self.parser.parse(query)
        result = self.executor.execute(parsed_query)
        assert "Jane Smith" not in result
        assert "John Doe" in result
    
    def test_create_index(self):
        """Test CREATE INDEX statement."""
        # Set up test data
        self._create_students_table()
        
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
    
    def test_drop_index(self):
        """Test DROP INDEX statement."""
        # Set up test data
        self._create_students_table()
        
        # Create index
        query = "CREATE INDEX ON students (name)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Now drop the index
        query = "DROP INDEX ON students (name)"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "DROP_INDEX"
        assert parsed_query["table_name"] == "students"
        assert parsed_query["column_name"] == "name"
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        assert "Index dropped" in result
        
        # Verify the index no longer exists
        assert not self.schema_manager.index_exists("students", "name")
    
    def test_drop_table(self):
        """Test DROP TABLE statement."""
        # First create a table
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
    
    def test_join(self):
        """Test JOIN operation."""
        # Set up test data - create students and courses tables
        self._create_students_table()
        self._insert_sample_students()
        
        # Create courses table
        query = """
        CREATE TABLE courses (
            course_id INTEGER PRIMARY KEY,
            title STRING,
            credits INTEGER
        )
        """
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Insert sample courses
        query = "INSERT INTO courses VALUES (101, 'Database Systems', 3)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        query = "INSERT INTO courses VALUES (102, 'Data Structures', 4)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Create enrollments table
        query = """
        CREATE TABLE enrollments (
            enrollment_id INTEGER PRIMARY KEY,
            student_id INTEGER,
            course_id INTEGER
        )
        """
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Insert sample enrollments
        query = "INSERT INTO enrollments VALUES (1, 1, 101)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        query = "INSERT INTO enrollments VALUES (2, 2, 102)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Test JOIN query
        query = """
        SELECT students.name, courses.title
        FROM students
        JOIN enrollments ON students.id = enrollments.student_id
        JOIN courses ON courses.course_id = enrollments.course_id
        """
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "SELECT"
        assert "join" in parsed_query
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        
        # Check for expected results in the join
        assert "John Doe" in result and "Database Systems" in result
        assert "Jane Smith" in result and "Data Structures" in result
    
    def test_join_with_where(self):
        """Test JOIN with WHERE clause."""
        # Set up test data - create students and courses tables
        self._create_students_table()
        self._insert_sample_students()
        
        # Create courses table
        query = """
        CREATE TABLE courses (
            course_id INTEGER PRIMARY KEY,
            title STRING,
            credits INTEGER
        )
        """
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Insert sample courses
        query = "INSERT INTO courses VALUES (101, 'Database Systems', 3)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        query = "INSERT INTO courses VALUES (102, 'Data Structures', 4)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Create enrollments table
        query = """
        CREATE TABLE enrollments (
            enrollment_id INTEGER PRIMARY KEY,
            student_id INTEGER,
            course_id INTEGER
        )
        """
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Insert sample enrollments
        query = "INSERT INTO enrollments VALUES (1, 1, 101)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        query = "INSERT INTO enrollments VALUES (2, 2, 102)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Test JOIN query with WHERE
        query = """
        SELECT students.name, courses.title
        FROM students
        JOIN enrollments ON students.id = enrollments.student_id
        JOIN courses ON courses.course_id = enrollments.course_id
        WHERE courses.credits > 3
        """
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "SELECT"
        assert "join" in parsed_query
        assert parsed_query["where"] is not None
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        
        # Check for expected results in the join with WHERE
        assert "Database Systems" not in result  # Has credits = 3
        assert "Data Structures" in result       # Has credits = 4
        assert "Jane Smith" in result            # Enrolled in Data Structures
        assert "John Doe" not in result          # Enrolled in Database Systems
    
    def test_show_tables(self):
        """Test SHOW TABLES statement."""
        # Create a few tables
        self._create_students_table()
        
        query = "CREATE TABLE courses (id INTEGER PRIMARY KEY, name STRING)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
        
        # Test SHOW TABLES
        query = "SHOW TABLES"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "SHOW_TABLES"
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        
        # Check that both tables are listed
        assert "students" in result
        assert "courses" in result
    
    def test_describe(self):
        """Test DESCRIBE statement."""
        # Create a table
        self._create_students_table()
        
        # Test DESCRIBE
        query = "DESCRIBE students"
        parsed_query = self.parser.parse(query)
        
        assert parsed_query["type"] == "DESCRIBE"
        assert parsed_query["table_name"] == "students"
        
        # Execute the query
        result = self.executor.execute(parsed_query)
        
        # Check that column information is displayed
        assert "id" in result
        assert "name" in result
        assert "age" in result
        assert "INTEGER" in result
        assert "STRING" in result
        assert "primary key" in result.lower() or "yes" in result.lower()
    
    def test_error_handling(self):
        """Test error handling."""
        # Create a table for testing
        self._create_students_table()
        
        # Test case 1: Try to create a table that already exists
        query = "CREATE TABLE students (id INTEGER PRIMARY KEY, name STRING)"
        parsed_query = self.parser.parse(query)
        
        with pytest.raises(DBMSError):
            self.executor.execute(parsed_query)
        
        # Test case 2: Try to select from a non-existent table
        query = "SELECT * FROM non_existent_table"
        parsed_query = self.parser.parse(query)
        
        with pytest.raises(DBMSError):
            self.executor.execute(parsed_query)
    
    # Helper methods
    def _create_students_table(self):
        """Helper to create students table."""
        query = "CREATE TABLE students (id INTEGER PRIMARY KEY, name STRING, age INTEGER)"
        parsed_query = self.parser.parse(query)
        self.executor.execute(parsed_query)
    
    def _insert_sample_students(self):
        """Helper to insert sample student data."""
        queries = [
            "INSERT INTO students VALUES (1, 'John Doe', 20)",
            "INSERT INTO students VALUES (2, 'Jane Smith', 22)"
        ]
        
        for query in queries:
            parsed_query = self.parser.parse(query)
            self.executor.execute(parsed_query)
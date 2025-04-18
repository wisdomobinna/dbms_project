"""
Test cases for auto-increment primary key functionality.
"""
import unittest
import os
import json
import shutil
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.executor import Executor
from catalog.schema_manager import SchemaManager
from storage.disk_manager import DiskManager
from storage.index.index_manager import IndexManager
from query.optimizer import QueryOptimizer
from parser.sql_parser import SQLParser

class TestAutoIncrementPK(unittest.TestCase):
    """Test cases for auto-increment primary key functionality."""

    def setUp(self):
        """Set up the test environment."""
        # Initialize test directory
        self.test_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test_db")
        
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        
        os.makedirs(self.test_dir)
        os.makedirs(os.path.join(self.test_dir, "data"))
        os.makedirs(os.path.join(self.test_dir, "indexes"))
        
        # Initialize components
        self.disk_manager = DiskManager(self.test_dir)
        self.schema_manager = SchemaManager(self.disk_manager)
        self.index_manager = IndexManager(self.disk_manager)
        self.optimizer = QueryOptimizer(self.schema_manager, self.index_manager)
        self.executor = Executor(self.schema_manager, self.disk_manager, self.index_manager, self.optimizer)
        self.parser = SQLParser()

    def tearDown(self):
        """Clean up after tests."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def execute_query(self, query):
        """Execute a SQL query and return the result."""
        parsed_query = self.parser.parse(query)
        return self.executor.execute(parsed_query)

    def test_create_table_with_auto_increment(self):
        """Test creating a table with an auto-increment primary key."""
        result = self.execute_query(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTO_INCREMENT, name STRING)"
        )
        self.assertIn("created successfully", result)
        
        # Check if the schema has the auto_increment flag
        with open(os.path.join(self.test_dir, "schema.json"), 'r') as f:
            schema = json.load(f)
        
        self.assertTrue(schema["columns"]["users"][0].get("auto_increment"))
        self.assertEqual(schema["primary_keys"]["users"], "id")

    def test_insert_with_auto_increment(self):
        """Test inserting records with auto-increment primary key."""
        # Create a table with auto-increment primary key
        self.execute_query(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY AUTO_INCREMENT, name STRING)"
        )
        
        # Insert records without providing ID values (null will trigger auto-increment)
        self.execute_query("INSERT INTO customers VALUES (0, 'John Doe')")
        self.execute_query("INSERT INTO customers VALUES (0, 'Jane Smith')")
        self.execute_query("INSERT INTO customers VALUES (0, 'Bob Johnson')")
        
        # Query the table to check if IDs were auto-assigned
        result = self.execute_query("SELECT * FROM customers")
        
        # Verify the result structure
        self.assertIn("rows", result)
        self.assertIn("columns", result)
        
        # Extract rows
        rows = result["rows"]
        
        # Should have 3 rows
        self.assertEqual(len(rows), 3)
        
        # Check if IDs were assigned sequentially
        ids = [int(row[0]) for row in rows]
        self.assertEqual(ids, [1, 2, 3])
        
        # Check if names were stored correctly
        names = [row[1] for row in rows]
        self.assertIn("John Doe", names)
        self.assertIn("Jane Smith", names)
        self.assertIn("Bob Johnson", names)

    def test_insert_with_explicit_id(self):
        """Test inserting records with explicitly provided ID values."""
        # Create a table with auto-increment primary key
        self.execute_query(
            "CREATE TABLE products (id INTEGER PRIMARY KEY AUTO_INCREMENT, name STRING, price INTEGER)"
        )
        
        # Insert records with explicit IDs
        self.execute_query("INSERT INTO products VALUES (10, 'Laptop', 1200)")
        self.execute_query("INSERT INTO products VALUES (20, 'Phone', 800)")
        
        # Now insert without explicit ID - should use next available ID above the max
        self.execute_query("INSERT INTO products VALUES (0, 'Tablet', 500)")
        
        # Query the table to check inserted records
        result = self.execute_query("SELECT * FROM products ORDER BY id")
        rows = result["rows"]
        
        # Check values
        self.assertEqual(int(rows[0][0]), 10)  # First explicit ID
        self.assertEqual(int(rows[1][0]), 20)  # Second explicit ID
        self.assertEqual(int(rows[2][0]), 21)  # Auto-assigned ID (should be max+1)

    def test_describe_shows_auto_increment(self):
        """Test that DESCRIBE command shows auto_increment attribute."""
        # Create a table with auto-increment primary key
        self.execute_query(
            "CREATE TABLE events (id INTEGER PRIMARY KEY AUTO_INCREMENT, name STRING, date STRING)"
        )
        
        # Describe the table
        result = self.execute_query("DESCRIBE events")
        
        # Check if the result contains auto_increment information
        found_auto_increment = False
        for row in result["rows"]:
            if row[0] == "id" and "Yes" in row:  # Look for Auto Increment: Yes
                auto_increment_column_index = result["columns"].index("Auto Increment")
                found_auto_increment = row[auto_increment_column_index] == "Yes"
        
        self.assertTrue(found_auto_increment, "Auto Increment attribute not found in DESCRIBE output")

if __name__ == '__main__':
    unittest.main()
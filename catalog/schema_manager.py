"""
Schema Manager Module

This module handles the database schema management, including:
- Table definitions
- Column definitions
- Primary and foreign key relationships
- Index tracking
"""

import os
import json
from common.exceptions import SchemaError
from common.types import DataType

class SchemaManager:
    """
    Schema Manager class that handles database metadata.
    """
    
    def __init__(self, disk_manager):
        """
        Initialize the Schema Manager.
        
        Args:
            disk_manager: The disk manager for persistent storage
        """
        self.disk_manager = disk_manager
        self.schema_file = os.path.join(disk_manager.db_directory, "schema.json")
        
        # Initialize schema structures
        self.tables = {}
        self.columns = {}
        self.indexes = {}
        self.primary_keys = {}
        self.foreign_keys = {}
    
    def load_schema(self):
        """Load the database schema from disk."""
        if os.path.exists(self.schema_file):
            try:
                with open(self.schema_file, 'r') as f:
                    schema_data = json.load(f)
                
                self.tables = schema_data.get("tables", {})
                self.columns = schema_data.get("columns", {})
                self.indexes = schema_data.get("indexes", {})
                self.primary_keys = schema_data.get("primary_keys", {})
                self.foreign_keys = schema_data.get("foreign_keys", {})
                
                # Convert string data types to enum values
                for table in self.columns:
                    for col in self.columns[table]:
                        if col["type"] == "INTEGER":
                            col["type"] = DataType.INTEGER
                        else:
                            col["type"] = DataType.STRING
                
                return True
            except Exception as e:
                print(f"Error loading schema: {str(e)}")
                return False
        
        return False
    
    def save_schema(self):
        """Save the database schema to disk."""
        try:
            # Convert enum data types to strings for JSON serialization
            serializable_columns = {}
            for table in self.columns:
                serializable_columns[table] = []
                for col in self.columns[table]:
                    col_copy = col.copy()
                    col_copy["type"] = "INTEGER" if col["type"] == DataType.INTEGER else "STRING"
                    serializable_columns[table].append(col_copy)
            
            schema_data = {
                "tables": self.tables,
                "columns": serializable_columns,
                "indexes": self.indexes,
                "primary_keys": self.primary_keys,
                "foreign_keys": self.foreign_keys
            }
            
            with open(self.schema_file, 'w') as f:
                json.dump(schema_data, f, indent=2)
            
            return True
        except Exception as e:
            print(f"Error saving schema: {str(e)}")
            return False
    
    def create_table(self, table_name, columns, primary_key=None, foreign_keys=None):
        """
        Create a new table in the schema.
        
        Args:
            table_name (str): Name of the table
            columns (list): List of column definitions
            primary_key (str, optional): Primary key column
            foreign_keys (dict, optional): Foreign key relationships
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            SchemaError: If there's an issue with the schema definition
        """
        if self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' already exists")
        
        # Validate columns
        column_names = [col["name"] for col in columns]
        if len(column_names) != len(set(column_names)):
            raise SchemaError("Duplicate column names are not allowed")
        
        # Validate primary key
        if primary_key and primary_key not in column_names:
            raise SchemaError(f"Primary key '{primary_key}' must be one of the columns")
        
        # Validate foreign keys
        if foreign_keys:
            for fk_col, fk_ref in foreign_keys.items():
                if fk_col not in column_names:
                    raise SchemaError(f"Foreign key column '{fk_col}' not defined in table")
                
                ref_table = fk_ref["table"]
                ref_col = fk_ref["column"]
                
                if not self.table_exists(ref_table):
                    raise SchemaError(f"Referenced table '{ref_table}' does not exist")
                
                if not self.column_exists(ref_table, ref_col):
                    raise SchemaError(f"Referenced column '{ref_col}' does not exist in table '{ref_table}'")
                
                if ref_col != self.get_primary_key(ref_table):
                    raise SchemaError("Foreign keys must reference primary key columns")
        
        # Create table files
        self.disk_manager.create_table_file(table_name)
        
        # Update schema
        self.tables[table_name] = {
            "name": table_name,
            "record_count": 0
        }
        
        self.columns[table_name] = columns
        
        if primary_key:
            self.primary_keys[table_name] = primary_key
            
            # Automatically create an index for the primary key
            self.create_index(table_name, primary_key)
        
        if foreign_keys:
            self.foreign_keys[table_name] = foreign_keys
        
        # Save schema to disk
        self.save_schema()
        
        return True
    
    def drop_table(self, table_name):
        """
        Drop a table from the schema.
        
        Args:
            table_name (str): Name of the table to drop
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            SchemaError: If the table doesn't exist or can't be dropped
        """
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
        
        # Check if any other tables have foreign keys referencing this table
        for other_table, fk_dict in self.foreign_keys.items():
            for fk_col, fk_ref in fk_dict.items():
                if fk_ref["table"] == table_name:
                    raise SchemaError(f"Cannot drop table '{table_name}' because it is referenced by table '{other_table}'")
        
        # Remove table files
        self.disk_manager.delete_table_file(table_name)
        
        # Remove indexes
        if table_name in self.indexes:
            for column in list(self.indexes[table_name]):
                self.disk_manager.delete_index_file(table_name, column)
            del self.indexes[table_name]
        
        # Update schema
        del self.tables[table_name]
        del self.columns[table_name]
        
        if table_name in self.primary_keys:
            del self.primary_keys[table_name]
        
        if table_name in self.foreign_keys:
            del self.foreign_keys[table_name]
        
        # Save schema to disk
        self.save_schema()
        
        return True
    
    def create_index(self, table_name, column_name):
        """
        Create an index for a column.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column to index
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            SchemaError: If there's an issue with creating the index
        """
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
        
        if not self.column_exists(table_name, column_name):
            raise SchemaError(f"Column '{column_name}' does not exist in table '{table_name}'")
        
        if self.index_exists(table_name, column_name):
            raise SchemaError(f"Index already exists for '{table_name}.{column_name}'")
        
        # Initialize indexes for the table if not present
        if table_name not in self.indexes:
            self.indexes[table_name] = []
        
        # Create index file
        self.disk_manager.create_index_file(table_name, column_name)
        
        # Update schema
        self.indexes[table_name].append(column_name)
        
        # Save schema to disk
        self.save_schema()
        
        return True
    
    def drop_index(self, table_name, column_name):
        """
        Drop an index for a column.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            SchemaError: If there's an issue with dropping the index
        """
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
        
        if not self.index_exists(table_name, column_name):
            raise SchemaError(f"No index exists for '{table_name}.{column_name}'")
        
        # Check if this is a primary key index (can't drop these)
        if table_name in self.primary_keys and self.primary_keys[table_name] == column_name:
            raise SchemaError(f"Cannot drop index for primary key column '{column_name}'")
        
        # Delete index file
        self.disk_manager.delete_index_file(table_name, column_name)
        
        # Update schema
        self.indexes[table_name].remove(column_name)
        
        # Clean up empty lists
        if not self.indexes[table_name]:
            del self.indexes[table_name]
        
        # Save schema to disk
        self.save_schema()
        
        return True
    
    def increment_record_count(self, table_name):
        """Increment the record count for a table."""
        if self.table_exists(table_name):
            self.tables[table_name]["record_count"] += 1
            self.save_schema()
    
    def decrement_record_count(self, table_name, count=1):
        """Decrement the record count for a table."""
        if self.table_exists(table_name):
            self.tables[table_name]["record_count"] -= count
            if self.tables[table_name]["record_count"] < 0:
                self.tables[table_name]["record_count"] = 0
            self.save_schema()
    
    def set_record_count(self, table_name, count):
        """Set the record count for a table."""
        if self.table_exists(table_name):
            self.tables[table_name]["record_count"] = count
            self.save_schema()
    
    def get_record_count(self, table_name):
        """Get the record count for a table."""
        if self.table_exists(table_name):
            return self.tables[table_name]["record_count"]
        return 0
    
    # Helper methods
    def table_exists(self, table_name):
        """Check if a table exists."""
        return table_name in self.tables
    
    def column_exists(self, table_name, column_name):
        """Check if a column exists in a table."""
        if not self.table_exists(table_name):
            return False
        
        return any(col["name"] == column_name for col in self.columns[table_name])
    
    def index_exists(self, table_name, column_name):
        """Check if an index exists for a column."""
        if not self.table_exists(table_name) or table_name not in self.indexes:
            return False
        
        return column_name in self.indexes[table_name]
    
    def primary_key_exists(self, table_name, value):
        """Check if a primary key value already exists in a table."""
        # This would typically use an index lookup
        return False  # Placeholder
    
    def foreign_key_exists(self, table_name, column_name, value):
        """Check if a value exists in a referenced table's column."""
        # This would typically use an index lookup
        return True  # Placeholder for now
    
    def get_tables(self):
        """Get all table names."""
        return list(self.tables.keys())
    
    def get_columns(self, table_name):
        """Get all columns for a table."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
        
        return self.columns[table_name]
    
    def get_column(self, table_name, column_name):
        """Get a specific column by name."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
        
        for col in self.columns[table_name]:
            if col["name"] == column_name:
                return col
        
        raise SchemaError(f"Column '{column_name}' does not exist in table '{table_name}'")
    
    def get_primary_key(self, table_name):
        """Get the primary key column name for a table."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
        
        return self.primary_keys.get(table_name)
    
    def get_foreign_keys(self, table_name):
        """Get all foreign key relationships for a table."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
        
        return self.foreign_keys.get(table_name, {})
    
    def get_indexes(self, table_name):
        """Get all indexed columns for a table."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
        
        return self.indexes.get(table_name, [])
    
    def get_table_info(self, table_name):
        """Get detailed information about a table."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
        
        return {
            "name": table_name,
            "columns": self.get_columns(table_name),
            "primary_key": self.get_primary_key(table_name),
            "foreign_keys": self.get_foreign_keys(table_name),
            "indexes": self.get_indexes(table_name),
            "record_count": self.get_record_count(table_name)
        }
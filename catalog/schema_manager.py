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
        
        # Auto-increment columns must be INTEGER and primary key
        for col in columns:
            if col.get("auto_increment"):
                if col["type"] != DataType.INTEGER:
                    raise SchemaError(f"Auto-increment column '{col['name']}' must be of INTEGER type")
                if not col.get("primary_key") and col["name"] != primary_key:
                    raise SchemaError(f"Auto-increment column '{col['name']}' must be a PRIMARY KEY")
        
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
        try:
            # Handle possible dict or other unhashable type
            if not isinstance(table_name, str):
                if hasattr(table_name, 'name'):
                    table_name = table_name.name
                elif isinstance(table_name, dict) and 'name' in table_name:
                    table_name = table_name['name']
                else:
                    # Couldn't find a valid table name
                    return 0
            
            # Check if table exists in schema
            if self.table_exists(table_name):
                return self.tables[table_name]["record_count"]
            return 0
        except (TypeError, KeyError):
            # Fallback for any unexpected errors
            return 0
    
    # Helper methods
    def table_exists(self, table_name):
        """Check if a table exists."""
        try:
            # Special case for derived tables (subqueries in the FROM clause)
            if isinstance(table_name, dict) and table_name.get('type') == 'derived_table':
                # Derived tables always "exist" - they are virtual tables created by subqueries
                return True
                
            # Handle possible dict or other unhashable type
            if not isinstance(table_name, str):
                if hasattr(table_name, 'name'):
                    table_name = table_name.name
                elif isinstance(table_name, dict) and 'name' in table_name:
                    table_name = table_name['name']
                else:
                    # Couldn't find a valid table name
                    return False
            
            # Handle 'table AS alias' format
            if isinstance(table_name, str) and " AS " in table_name.upper():
                # Extract just the table name before the AS keyword
                real_table = table_name.split(" AS ", 1)[0].strip()
                return real_table in self.tables
            
            return table_name in self.tables
        except (TypeError, KeyError):
            # Fallback for any unexpected errors
            return False
    
    def column_exists(self, table_name, column_name):
        """Check if a column exists in a table."""
        if not self.table_exists(table_name):
            return False
            
        # Handle alias.column format for join conditions
        if "." in column_name:
            # Extract just the column part
            parts = column_name.split(".")
            if len(parts) == 2:
                # We have alias.column format
                alias = parts[0]
                pure_column = parts[1]
                
                # If table_name is a dict with an alias matching the alias part (case insensitive)
                if isinstance(table_name, dict) and table_name.get('alias', '').lower() == alias.lower():
                    real_table_name = table_name.get('name')
                    return self.column_exists(real_table_name, pure_column)
                
                # If table_name is a string and we have a "table AS alias" format (case insensitive)
                if isinstance(table_name, str) and " AS " in table_name.upper():
                    table_parts = table_name.split(" AS ", 1)
                    real_table = table_parts[0].strip()
                    table_alias = table_parts[1].strip()
                    
                    # Compare aliases case-insensitively
                    if table_alias.lower() == alias.lower():
                        return self.column_exists(real_table, pure_column)
                
                # Handle the case where the table name itself is the alias
                # Just check if the column exists in the table, don't return False immediately
                return self.column_exists(table_name, pure_column)
                
        # Extract the real table name if in 'table AS alias' format
        actual_table_name = table_name
        if isinstance(table_name, str) and " AS " in table_name.upper():
            actual_table_name = table_name.split(" AS ", 1)[0].strip()
            
        # Special case for derived tables (subqueries in the FROM clause)
        if isinstance(table_name, dict) and table_name.get('type') == 'derived_table':
            # For derived tables, get columns from the subquery's projection
            subquery = table_name.get('subquery', {})
            projection = subquery.get('projection', {})
            
            if projection.get('type') == 'all':
                # SELECT * - we need to check the base table
                base_table = subquery.get('table')
                return self.column_exists(base_table, column_name)
            elif projection.get('type') == 'columns':
                # Check for exact column match or alias
                columns = projection.get('columns', [])
                for col in columns:
                    if col.get('type') == 'column' and (col.get('name') == column_name or col.get('alias') == column_name):
                        return True
                    elif col.get('type') == 'aggregation' and col.get('alias') == column_name:
                        return True
            
            # If column not found in projection, it doesn't exist in derived table
            return False
        
        # Special case for table alias dictionary
        if isinstance(table_name, dict) and "name" in table_name:
            real_table_name = table_name["name"]
            return self.column_exists(real_table_name, column_name)
            
        # Regular tables - check schema
        try:
            # Only proceed if we have a string table name
            if not isinstance(actual_table_name, str):
                return False
                
            if actual_table_name not in self.columns:
                return False
                
            return any(col["name"] == column_name for col in self.columns[actual_table_name])
        except Exception as e:
            print(f"Error checking column existence: {e}")
            return False
    
    def index_exists(self, table_name, column_name):
        """Check if an index exists for a column."""
        # Special case for derived tables
        if isinstance(table_name, dict) and table_name.get('type') == 'derived_table':
            # Derived tables don't have indexes
            return False
            
        # Convert table_name to a string if it's not already
        if not isinstance(table_name, str):
            if hasattr(table_name, 'name'):
                table_name = table_name.name
            elif isinstance(table_name, dict) and 'name' in table_name:
                table_name = table_name['name']
                
        if not self.table_exists(table_name) or table_name not in self.indexes:
            return False
        
        return column_name in self.indexes[table_name]
    
    def primary_key_exists(self, table_name, value):
        """
        Check if a primary key value already exists in a table.
        
        Args:
            table_name (str): Name of the table
            value: The primary key value to check
            
        Returns:
            bool: True if the primary key value exists, False otherwise
        """
        pk_column = self.get_primary_key(table_name)
        if not pk_column:
            return False
            
        # Use the index manager to look up the value in the primary key index
        if not hasattr(self, 'index_manager'):
            # Lazy initialize the index manager if not already available
            from storage.index.index_manager import IndexManager
            self.index_manager = IndexManager(self.disk_manager)
            
        # Look up the value in the primary key index
        if self.index_exists(table_name, pk_column):
            record_ids = self.index_manager.lookup(table_name, pk_column, value)
            return len(record_ids) > 0
            
        # Fall back to a table scan if no index exists
        try:
            records = self.disk_manager.read_table(table_name)
            for record in records:
                if not record.get("__deleted__", False) and record.get(pk_column) == value:
                    return True
            return False
        except:
            return False
    
    def foreign_key_exists(self, table_name, column_name, value):
        """
        Check if a value exists in a referenced table's column.
        
        Args:
            table_name (str): Name of the referenced table
            column_name (str): Name of the referenced column
            value: The value to check
            
        Returns:
            bool: True if the value exists in the referenced column, False otherwise
        """
        if not self.table_exists(table_name) or not self.column_exists(table_name, column_name):
            return False
            
        # Use the index manager to look up the value in the referenced column
        if not hasattr(self, 'index_manager'):
            # Lazy initialize the index manager if not already available
            from storage.index.index_manager import IndexManager
            self.index_manager = IndexManager(self.disk_manager)
            
        # Look up the value in the index if it exists
        if self.index_exists(table_name, column_name):
            record_ids = self.index_manager.lookup(table_name, column_name, value)
            return len(record_ids) > 0
            
        # Fall back to a table scan if no index exists
        try:
            records = self.disk_manager.read_table(table_name)
            for record in records:
                if not record.get("__deleted__", False) and record.get(column_name) == value:
                    return True
            return False
        except:
            return False
    
    def get_tables(self):
        """Get all table names."""
        return list(self.tables.keys())
    
    def get_columns(self, table_name):
        """Get all columns for a table."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
            
        # Special case for derived tables (subqueries in the FROM clause)
        if isinstance(table_name, dict) and table_name.get('type') == 'derived_table':
            # Generate column information from the subquery's projection
            subquery = table_name.get('subquery', {})
            projection = subquery.get('projection', {})
            columns = []
            
            if projection.get('type') == 'all':
                # SELECT * - use the columns from the base table
                base_table = subquery.get('table')
                return self.get_columns(base_table)
            elif projection.get('type') == 'columns':
                # Transform the projection columns to schema columns
                for col in projection.get('columns', []):
                    if col.get('type') == 'column':
                        # Regular column
                        column_name = col.get('alias', col.get('name'))
                        # Try to determine column type from original table if possible
                        column_type = DataType.STRING  # Default type
                        try:
                            orig_table = subquery.get('table')
                            orig_column = col.get('name')
                            if '.' in orig_column:
                                parts = orig_column.split('.')
                                orig_table, orig_column = parts[0], parts[1]
                            orig_col_info = self.get_column(orig_table, orig_column)
                            column_type = orig_col_info.get('type', DataType.STRING)
                        except:
                            pass  # If we can't determine the type, use STRING as default
                            
                        columns.append({
                            'name': column_name,
                            'type': column_type
                        })
                    elif col.get('type') == 'aggregation':
                        # Aggregation function (COUNT, SUM, etc.)
                        column_name = col.get('alias', f"{col.get('function')}({col.get('argument')})")
                        # Most aggregates return INTEGER
                        column_type = DataType.INTEGER
                        columns.append({
                            'name': column_name,
                            'type': column_type
                        })
                return columns
                
        # Regular tables - get columns from schema
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
            
        # Special case for derived tables (subqueries in the FROM clause)
        if isinstance(table_name, dict) and table_name.get('type') == 'derived_table':
            # Derived tables don't have primary keys
            return None
            
        # Convert table_name to a string if it's not already
        if not isinstance(table_name, str):
            if hasattr(table_name, 'name'):
                table_name = table_name.name
            elif isinstance(table_name, dict) and 'name' in table_name:
                table_name = table_name['name']
        
        return self.primary_keys.get(table_name)
    
    def get_foreign_keys(self, table_name):
        """Get all foreign key relationships for a table."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
            
        # Special case for derived tables (subqueries in the FROM clause)
        if isinstance(table_name, dict) and table_name.get('type') == 'derived_table':
            # Derived tables don't have foreign keys
            return {}
            
        # Convert table_name to a string if it's not already
        if not isinstance(table_name, str):
            if hasattr(table_name, 'name'):
                table_name = table_name.name
            elif isinstance(table_name, dict) and 'name' in table_name:
                table_name = table_name['name']
        
        return self.foreign_keys.get(table_name, {})
    
    def get_indexes(self, table_name):
        """Get all indexed columns for a table."""
        if not self.table_exists(table_name):
            raise SchemaError(f"Table '{table_name}' does not exist")
            
        # Special case for derived tables (subqueries in the FROM clause)
        if isinstance(table_name, dict) and table_name.get('type') == 'derived_table':
            # Derived tables don't have indexes
            return []
            
        # Convert table_name to a string if it's not already
        if not isinstance(table_name, str):
            if hasattr(table_name, 'name'):
                table_name = table_name.name
            elif isinstance(table_name, dict) and 'name' in table_name:
                table_name = table_name['name']
        
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
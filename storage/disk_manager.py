"""
Disk Manager Module

This module handles low-level file I/O operations for tables and indexes.
"""

import os
import json
import pickle
from common.exceptions import StorageError

class DiskManager:
    """
    Disk Manager class that handles storage operations.
    """
    
    def __init__(self, db_directory):
        """
        Initialize the Disk Manager.
        
        Args:
            db_directory (str): Directory to store database files
        """
        self.db_directory = db_directory
        self.data_directory = os.path.join(db_directory, "data")
        self.index_directory = os.path.join(db_directory, "indexes")
        
        # Create directories if they don't exist
        os.makedirs(self.data_directory, exist_ok=True)
        os.makedirs(self.index_directory, exist_ok=True)
    
    def get_table_path(self, table_name):
        """Get the file path for a table."""
        return os.path.join(self.data_directory, f"{table_name}.dat")
    
    def get_index_path(self, table_name, column_name):
        """Get the file path for an index."""
        return os.path.join(self.index_directory, f"{table_name}_{column_name}.idx")
    
    def create_table_file(self, table_name):
        """
        Create a new table file.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            table_path = self.get_table_path(table_name)
            
            # Initialize with an empty list of records
            with open(table_path, 'wb') as f:
                pickle.dump([], f)
            
            return True
        except Exception as e:
            raise StorageError(f"Error creating table file: {str(e)}")
    
    def delete_table_file(self, table_name):
        """
        Delete a table file.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            table_path = self.get_table_path(table_name)
            
            if os.path.exists(table_path):
                os.remove(table_path)
            
            return True
        except Exception as e:
            raise StorageError(f"Error deleting table file: {str(e)}")
    
    def create_index_file(self, table_name, column_name):
        """
        Create a new index file.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            index_path = self.get_index_path(table_name, column_name)
            
            # Initialize with an empty dictionary (key -> record_id)
            with open(index_path, 'wb') as f:
                pickle.dump({}, f)
            
            return True
        except Exception as e:
            raise StorageError(f"Error creating index file: {str(e)}")
    
    def delete_index_file(self, table_name, column_name):
        """
        Delete an index file.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            index_path = self.get_index_path(table_name, column_name)
            
            if os.path.exists(index_path):
                os.remove(index_path)
            
            return True
        except Exception as e:
            raise StorageError(f"Error deleting index file: {str(e)}")
    
    def read_table(self, table_name):
        """
        Read all records from a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            list: List of records
        """
        try:
            table_path = self.get_table_path(table_name)
            
            if not os.path.exists(table_path):
                raise StorageError(f"Table file for '{table_name}' does not exist")
            
            with open(table_path, 'rb') as f:
                records = pickle.load(f)
            
            return records
        except Exception as e:
            raise StorageError(f"Error reading table: {str(e)}")
    
    def write_table(self, table_name, records):
        """
        Write records to a table.
        
        Args:
            table_name (str): Name of the table
            records (list): List of records
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            table_path = self.get_table_path(table_name)
            
            with open(table_path, 'wb') as f:
                pickle.dump(records, f)
            
            return True
        except Exception as e:
            raise StorageError(f"Error writing table: {str(e)}")
    
    def read_index(self, table_name, column_name):
        """
        Read an index.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            
        Returns:
            dict: Index mapping (key -> record_id)
        """
        try:
            index_path = self.get_index_path(table_name, column_name)
            
            if not os.path.exists(index_path):
                raise StorageError(f"Index file for '{table_name}.{column_name}' does not exist")
            
            with open(index_path, 'rb') as f:
                index = pickle.load(f)
            
            return index
        except Exception as e:
            raise StorageError(f"Error reading index: {str(e)}")
    
    def write_index(self, table_name, column_name, index):
        """
        Write an index.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            index (dict): Index mapping (key -> record_id)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            index_path = self.get_index_path(table_name, column_name)
            
            with open(index_path, 'wb') as f:
                pickle.dump(index, f)
            
            return True
        except Exception as e:
            raise StorageError(f"Error writing index: {str(e)}")
    
    def insert_record(self, table_name, record):
        """
        Insert a record into a table.
        
        Args:
            table_name (str): Name of the table
            record (dict): Record to insert
            
        Returns:
            int: Record ID
        """
        try:
            records = self.read_table(table_name)
            
            # Generate a new record ID
            record_id = len(records)
            
            # Add record ID to the record
            record["__id__"] = record_id
            
            # Append the new record
            records.append(record)
            
            # Write back to disk
            self.write_table(table_name, records)
            
            return record_id
        except Exception as e:
            raise StorageError(f"Error inserting record: {str(e)}")
    
    def update_record(self, table_name, record_id, record):
        """
        Update a record in a table.
        
        Args:
            table_name (str): Name of the table
            record_id (int): ID of the record to update
            record (dict): New record data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            records = self.read_table(table_name)
            
            if record_id < 0 or record_id >= len(records):
                raise StorageError(f"Invalid record ID: {record_id}")
            
            # Preserve the record ID
            record["__id__"] = record_id
            
            # Update the record
            records[record_id] = record
            
            # Write back to disk
            self.write_table(table_name, records)
            
            return True
        except Exception as e:
            raise StorageError(f"Error updating record: {str(e)}")
    
    def delete_record(self, table_name, record_id):
        """
        Delete a record from a table.
        
        Args:
            table_name (str): Name of the table
            record_id (int): ID of the record to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            records = self.read_table(table_name)
            
            if record_id < 0 or record_id >= len(records):
                raise StorageError(f"Invalid record ID: {record_id}")
            
            # Mark the record as deleted (rather than removing it)
            records[record_id]["__deleted__"] = True
            
            # Write back to disk
            self.write_table(table_name, records)
            
            return True
        except Exception as e:
            raise StorageError(f"Error deleting record: {str(e)}")
    
    def get_record(self, table_name, record_id):
        """
        Get a record by ID.
        
        Args:
            table_name (str): Name of the table
            record_id (int): ID of the record
            
        Returns:
            dict: Record data
        """
        try:
            records = self.read_table(table_name)
            
            if record_id < 0 or record_id >= len(records):
                raise StorageError(f"Invalid record ID: {record_id}")
            
            record = records[record_id]
            
            # Check if the record is deleted
            if record.get("__deleted__", False):
                raise StorageError(f"Record {record_id} is deleted")
            
            return record
        except Exception as e:
            raise StorageError(f"Error getting record: {str(e)}")
    
    def vacuum_table(self, table_name):
        """
        Remove deleted records from a table and reindex.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            int: Number of records removed
        """
        try:
            records = self.read_table(table_name)
            
            # Filter out deleted records
            active_records = [r for r in records if not r.get("__deleted__", False)]
            
            # Reassign record IDs
            for i, record in enumerate(active_records):
                record["__id__"] = i
            
            # Write back to disk
            self.write_table(table_name, active_records)
            
            # Return number of removed records
            return len(records) - len(active_records)
        except Exception as e:
            raise StorageError(f"Error vacuuming table: {str(e)}")
    
    def rebuild_index(self, table_name, column_name):
        """
        Rebuild an index from scratch.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            records = self.read_table(table_name)
            
            # Create a new index
            index = {}
            
            # Build the index
            for record in records:
                if not record.get("__deleted__", False):
                    key = record.get(column_name)
                    record_id = record["__id__"]
                    
                    # Handle duplicate keys (convert to list)
                    if key in index:
                        if isinstance(index[key], list):
                            index[key].append(record_id)
                        else:
                            index[key] = [index[key], record_id]
                    else:
                        index[key] = record_id
            
            # Write the index to disk
            self.write_index(table_name, column_name, index)
            
            return True
        except Exception as e:
            raise StorageError(f"Error rebuilding index: {str(e)}")
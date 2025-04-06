"""
Index Manager Module

This module handles database index operations, including:
- Index creation and maintenance
- Index-based record lookup
- Primary key index management
"""

import os
from common.exceptions import IndexError

class IndexManager:
    """
    Index Manager class that handles database indexes.
    """
    
    def __init__(self, disk_manager):
        """
        Initialize the Index Manager.
        
        Args:
            disk_manager: The disk manager for index file operations
        """
        self.disk_manager = disk_manager
    
    def create_index(self, table_name, column_name):
        """
        Create a new index for a column.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column to index
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create the index file
            self.disk_manager.create_index_file(table_name, column_name)
            
            # Build the initial index
            self.rebuild_index(table_name, column_name)
            
            return True
        except Exception as e:
            raise IndexError(f"Error creating index: {str(e)}")
    
    def drop_index(self, table_name, column_name):
        """
        Drop an index.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Delete the index file
            self.disk_manager.delete_index_file(table_name, column_name)
            
            return True
        except Exception as e:
            raise IndexError(f"Error dropping index: {str(e)}")
    
    def rebuild_index(self, table_name, column_name):
        """
        Rebuild an index from the table data.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.disk_manager.rebuild_index(table_name, column_name)
        except Exception as e:
            raise IndexError(f"Error rebuilding index: {str(e)}")
    
    def update_index(self, table_name, column_name, key, record_id, old_key=None):
        """
        Update an index entry.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            key: The key value
            record_id (int): Record ID
            old_key: Previous key value (for updates)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Read the current index
            index = self.disk_manager.read_index(table_name, column_name)
            
            # Remove old key if provided
            if old_key is not None and old_key in index:
                if isinstance(index[old_key], list):
                    # Remove record ID from the list
                    index[old_key] = [rid for rid in index[old_key] if rid != record_id]
                    # If the list becomes empty, remove the key
                    if not index[old_key]:
                        del index[old_key]
                elif index[old_key] == record_id:
                    # Remove the key entirely
                    del index[old_key]
            
            # Add new key
            if key in index:
                if isinstance(index[key], list):
                    # Add to existing list if not already present
                    if record_id not in index[key]:
                        index[key].append(record_id)
                else:
                    # Convert to list if different record ID
                    if index[key] != record_id:
                        index[key] = [index[key], record_id]
            else:
                # New key
                index[key] = record_id
            
            # Write the updated index back to disk
            self.disk_manager.write_index(table_name, column_name, index)
            
            return True
        except Exception as e:
            raise IndexError(f"Error updating index: {str(e)}")
    
    def delete_from_index(self, table_name, column_name, key, record_id):
        """
        Remove a record from an index.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            key: The key value
            record_id (int): Record ID
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Read the current index
            index = self.disk_manager.read_index(table_name, column_name)
            
            # Remove the record ID from the index
            if key in index:
                if isinstance(index[key], list):
                    # Remove from list
                    index[key] = [rid for rid in index[key] if rid != record_id]
                    # If the list becomes empty, remove the key
                    if not index[key]:
                        del index[key]
                elif index[key] == record_id:
                    # Remove the key entirely
                    del index[key]
            
            # Write the updated index back to disk
            self.disk_manager.write_index(table_name, column_name, index)
            
            return True
        except Exception as e:
            raise IndexError(f"Error deleting from index: {str(e)}")
    
    def lookup(self, table_name, column_name, key, operators=None):
        """
        Look up records by key in an index.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            key: The key value to look up
            operators (str, optional): Comparison operator ("=", "<", ">", "<=", ">=")
            
        Returns:
            list: List of record IDs
        """
        try:
            # Read the index
            index = self.disk_manager.read_index(table_name, column_name)
            
            # Default operator is equality
            if operators is None or operators == "=":
                # Exact match
                if key in index:
                    if isinstance(index[key], list):
                        return index[key]
                    else:
                        return [index[key]]
                return []
            
            # Handle other comparison operators
            result = []
            for k, v in index.items():
                if operators == "<" and k < key:
                    result.extend([v] if not isinstance(v, list) else v)
                elif operators == "<=" and k <= key:
                    result.extend([v] if not isinstance(v, list) else v)
                elif operators == ">" and k > key:
                    result.extend([v] if not isinstance(v, list) else v)
                elif operators == ">=" and k >= key:
                    result.extend([v] if not isinstance(v, list) else v)
                elif operators == "!=" and k != key:
                    result.extend([v] if not isinstance(v, list) else v)
            
            return result
        except Exception as e:
            raise IndexError(f"Error looking up in index: {str(e)}")
    
    def range_lookup(self, table_name, column_name, start_key, end_key, inclusive=True):
        """
        Look up records within a key range.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            start_key: The lower bound
            end_key: The upper bound
            inclusive (bool): Whether to include the bounds
            
        Returns:
            list: List of record IDs
        """
        try:
            # Read the index
            index = self.disk_manager.read_index(table_name, column_name)
            
            result = []
            for k, v in index.items():
                if inclusive:
                    if start_key <= k <= end_key:
                        result.extend([v] if not isinstance(v, list) else v)
                else:
                    if start_key < k < end_key:
                        result.extend([v] if not isinstance(v, list) else v)
            
            return result
        except Exception as e:
            raise IndexError(f"Error looking up range in index: {str(e)}")
    
    def get_all_keys(self, table_name, column_name):
        """
        Get all keys in an index.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            
        Returns:
            list: List of all keys
        """
        try:
            # Read the index
            index = self.disk_manager.read_index(table_name, column_name)
            
            return list(index.keys())
        except Exception as e:
            raise IndexError(f"Error getting all keys: {str(e)}")
    
    def get_key_count(self, table_name, column_name):
        """
        Get the number of unique keys in an index.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the indexed column
            
        Returns:
            int: Number of unique keys
        """
        try:
            # Read the index
            index = self.disk_manager.read_index(table_name, column_name)
            
            return len(index)
        except Exception as e:
            raise IndexError(f"Error getting key count: {str(e)}")
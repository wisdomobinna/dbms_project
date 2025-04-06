#!/usr/bin/env python3
"""
DBMS Implementation Project - Main Entry Point
This file contains the main application class and command-line interface.
"""

import os
import sys
import time
import argparse
import readline  # For command history and editing capabilities

from dbms_project.parser.sql_parser import SQLParser
from catalog.schema_manager import SchemaManager
from storage.disk_manager import DiskManager
from storage.index.index_manager import IndexManager
from query.optimizer import QueryOptimizer
from execution.executor import Executor

class DBMSApplication:
    """Main DBMS application class that coordinates all components."""
    
    def __init__(self, db_directory="./database"):
        """
        Initialize the DBMS application.
        
        Args:
            db_directory (str): Directory to store database files
        """
        # Ensure database directory exists
        os.makedirs(db_directory, exist_ok=True)
        
        # Initialize components
        self.disk_manager = DiskManager(db_directory)
        self.schema_manager = SchemaManager(self.disk_manager)
        self.index_manager = IndexManager(self.disk_manager)
        self.parser = SQLParser()
        self.optimizer = QueryOptimizer(self.schema_manager, self.index_manager)
        self.executor = Executor(
            self.schema_manager, 
            self.disk_manager,
            self.index_manager,
            self.optimizer
        )
        
        # Load existing database schema if any
        self.schema_manager.load_schema()
        
    def run_query(self, query):
        """
        Parse, optimize and execute a query, measuring execution time.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            tuple: (result, execution_time)
        """
        start_time = time.time()
        
        try:
            # Parse the query
            parsed_query = self.parser.parse(query)
            
            # Validate the query against the schema
            self.parser.validate(parsed_query, self.schema_manager)
            
            # Optimize the query if it's a SELECT
            if parsed_query["type"] == "SELECT":
                parsed_query = self.optimizer.optimize(parsed_query)
            
            # Execute the query
            result = self.executor.execute(parsed_query)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            return result, execution_time
        except Exception as e:
            return f"Error: {str(e)}", time.time() - start_time

    def run_script(self, script_path):
        """
        Execute SQL statements from a script file.
        
        Args:
            script_path (str): Path to the SQL script file
        """
        try:
            with open(script_path, 'r') as f:
                script_content = f.read()
            
            # Split script into individual statements
            statements = script_content.split(';')
            
            results = []
            for stmt in statements:
                stmt = stmt.strip()
                if stmt:  # Skip empty statements
                    result, execution_time = self.run_query(stmt)
                    results.append((stmt, result, execution_time))
            
            # Print results
            for stmt, result, execution_time in results:
                print(f"\nQuery: {stmt}")
                print(f"Result: {result}")
                print(f"Execution time: {execution_time:.6f} seconds")
            
            return True
        except Exception as e:
            print(f"Error executing script: {str(e)}")
            return False

    def start_cli(self):
        """Start the command-line interface."""
        print("MiniDBMS - Database Management System Implementation")
        print("Type 'exit' or 'quit' to exit")
        print("Type 'help' for available commands")
        
        # Setup readline for command history
        histfile = os.path.join(os.path.expanduser("~"), ".minidbms_history")
        try:
            readline.read_history_file(histfile)
            readline.set_history_length(1000)
        except FileNotFoundError:
            pass
        
        try:
            while True:
                try:
                    query = input("dbms> ")
                    readline.add_history(query)
                    
                    if query.lower() in ('exit', 'quit'):
                        break
                    elif query.lower() == 'help':
                        self._print_help()
                    elif query.lower().startswith('run '):
                        # Run script command
                        script_path = query[4:].strip()
                        self.run_script(script_path)
                    elif query.strip():
                        result, execution_time = self.run_query(query)
                        print(f"\nResult: {result}")
                        print(f"Execution time: {execution_time:.6f} seconds\n")
                except KeyboardInterrupt:
                    print("\nUse 'exit' or 'quit' to exit")
                except Exception as e:
                    print(f"Error: {str(e)}")
        finally:
            # Save command history
            readline.write_history_file(histfile)
    
    def _print_help(self):
        """Print available commands and their descriptions."""
        print("\nAvailable SQL Commands:")
        print("  CREATE TABLE table_name (column_name type [PRIMARY KEY], ...)")
        print("  DROP TABLE table_name")
        print("  CREATE INDEX ON table_name (column_name)")
        print("  DROP INDEX ON table_name (column_name)")
        print("  SELECT columns FROM table [WHERE conditions] [ORDER BY columns] [HAVING condition]")
        print("  INSERT INTO table_name VALUES (value1, value2, ...)")
        print("  UPDATE table_name SET column=value [WHERE conditions]")
        print("  DELETE FROM table_name [WHERE conditions]")
        
        print("\nSpecial Commands:")
        print("  run <file_path> - Execute SQL statements from a file")
        print("  exit/quit - Exit the application")
        print("  help - Show this help message\n")

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='MiniDBMS - DBMS Implementation Project')
    parser.add_argument('--db-dir', default='./database', 
                        help='Directory to store database files')
    parser.add_argument('--script', help='SQL script file to execute')
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    app = DBMSApplication(db_directory=args.db_dir)
    
    if args.script:
        app.run_script(args.script)
    else:
        app.start_cli()
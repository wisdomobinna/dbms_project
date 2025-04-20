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
from colorama import init, Fore, Style  # For colored output

# Fix imports to use relative imports without package
# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parser.sql_parser import SQLParser
from catalog.schema_manager import SchemaManager
from storage.disk_manager import DiskManager
from storage.index.index_manager import IndexManager
from query.optimizer import QueryOptimizer
from execution.executor import Executor
from common.types import DataType

# Initialize colorama
init()

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
                import json
                print("Parsed query (before optimizer):")
                print(json.dumps(parsed_query, indent=2))
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
                    print(f"\n{Fore.CYAN}Executing:{Style.RESET_ALL} {stmt}")
                    result, execution_time = self.run_query(stmt)
                    results.append((stmt, result, execution_time))
            
            # Print results
            for stmt, result, execution_time in results:
                print(f"\n{Fore.GREEN}Query:{Style.RESET_ALL} {stmt}")
                print(f"{Fore.GREEN}Result:{Style.RESET_ALL}")
                
                # Format tabular results nicely
                if isinstance(result, str) and '\n' in result:
                    print(self._format_table_output(result))
                else:
                    print(result)
                    
                print(f"{Fore.GREEN}Execution time:{Style.RESET_ALL} {execution_time:.6f} seconds")
            
            return True
        except Exception as e:
            print(f"{Fore.RED}Error executing script: {str(e)}{Style.RESET_ALL}")
            return False

    def _format_table_output(self, result):
        """Format tabular output with colors for better readability."""
        lines = result.split('\n')
        if len(lines) >= 3:  # Has header, separator and data
            formatted = [f"{Fore.YELLOW}{lines[0]}{Style.RESET_ALL}"]  # Header in yellow
            formatted.append(lines[1])  # Separator line
            
            # Data rows in white
            for i in range(2, len(lines)):
                formatted.append(lines[i])
                
            return '\n'.join(formatted)
        return result

    def start_cli(self):
        """Start the command-line interface."""
        print(f"{Fore.CYAN}╔══════════════════════════════════════════════════════════╗{Style.RESET_ALL}")
        print(f"{Fore.CYAN}║ {Fore.WHITE}MiniDBMS - Database Management System Implementation{Fore.CYAN} ║{Style.RESET_ALL}")
        print(f"{Fore.CYAN}╚══════════════════════════════════════════════════════════╝{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Type 'exit' or 'quit' to exit{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Type 'help' for available commands{Style.RESET_ALL}")
        
        # Show available tables if any
        tables = self.schema_manager.get_tables()
        if tables:
            print(f"\n{Fore.GREEN}Available tables: {', '.join(tables)}{Style.RESET_ALL}")
        
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
                    # Collect multiline input
                    query_lines = []
                    prompt = f"{Fore.CYAN}dbms>{Style.RESET_ALL} "
                    continuation_prompt = f"{Fore.CYAN}...>{Style.RESET_ALL} "
                    
                    first_line = input(prompt)
                    
                    # Check for exit/quit immediately
                    if first_line.lower().strip() in ('exit', 'quit'):
                        print(f"{Fore.YELLOW}Goodbye!{Style.RESET_ALL}")
                        sys.exit(0)
                        
                    if first_line.strip():
                        query_lines.append(first_line)
                        
                        # Continue collecting lines until empty line or semicolon at end
                        while True:
                            line = input(continuation_prompt)
                            if not line.strip():
                                break
                            
                            query_lines.append(line)
                            if line.strip().endswith(';'):
                                break
                    
                    query = '\n'.join(query_lines)
                    
                    if query:
                        readline.add_history(query)
                    
                    if query.lower() == 'help':
                        self._print_help()
                    elif query.lower() == 'tables':
                        # Shortcut to list tables
                        tables = self.schema_manager.get_tables()
                        if tables:
                            print(f"\n{Fore.GREEN}Available tables:{Style.RESET_ALL}")
                            for table in tables:
                                columns = self.schema_manager.get_columns(table)
                                col_info = [f"{col['name']} ({col['type']})" for col in columns]
                                print(f"  {Fore.YELLOW}{table}{Style.RESET_ALL}: {', '.join(col_info)}")
                        else:
                            print(f"{Fore.YELLOW}No tables defined yet.{Style.RESET_ALL}")
                    elif query.lower().startswith('run '):
                        # Run script command
                        script_path = query[4:].strip()
                        self.run_script(script_path)
                    elif query.lower().startswith('copy '):
                        # Handle COPY command for bulk data import
                        self._execute_copy_command(query)
                    elif query.strip():
                        result, execution_time = self.run_query(query)
                        print(f"\n{Fore.GREEN}Result:{Style.RESET_ALL}")
                        
                        # Format tabular results nicely
                        if isinstance(result, str) and '\n' in result:
                            print(self._format_table_output(result))
                        else:
                            print(result)
                        
                        print(f"{Fore.GREEN}Execution time:{Style.RESET_ALL} {execution_time:.6f} seconds\n")
                except KeyboardInterrupt:
                    print(f"\n{Fore.YELLOW}Use 'exit' or 'quit' to exit{Style.RESET_ALL}")
                except Exception as e:
                    print(f"{Fore.RED}Error: {str(e)}{Style.RESET_ALL}")
        finally:
            # Save command history
            readline.write_history_file(histfile)
            
    def _execute_copy_command(self, command):
        """
        Execute a COPY command for bulk data import.

        Syntax: COPY table_name FROM 'filename' [DELIMITER 'delimiter'] [CSV]
        """
        import csv
        import re

        try:
            # Extract table name and filename
            copy_match = re.match(r'COPY\s+(\w+)\s+FROM\s+[\'"]([^\'"]+)[\'"]', command, re.IGNORECASE)
            if not copy_match:
                raise ValueError("Invalid COPY command syntax. Expected: COPY table FROM 'filename' [DELIMITER 'delimiter'] [CSV]")

            table_name = copy_match.group(1)
            filename = copy_match.group(2)

            # Confirm table exists
            if not self.schema_manager.table_exists(table_name):
                raise ValueError(f"Table '{table_name}' does not exist")

            # Extract delimiter
            delimiter = ','
            delimiter_match = re.search(r'DELIMITER\s+[\'"]([^\'"]+)[\'"]', command, re.IGNORECASE)
            if delimiter_match:
                delimiter = delimiter_match.group(1)

            is_csv = 'CSV' in command.upper()

            columns = self.schema_manager.get_columns(table_name)

            # Read file
            with open(filename, 'r') as f:
                if is_csv:
                    reader = csv.reader(f, delimiter=delimiter)
                    rows = list(reader)
                    if rows and rows[0][0].strip().lower() == columns[0]['name'].lower():
                        rows = rows[1:]
                else:
                    rows = [line.strip().split(delimiter) for line in f if line.strip()]

            values_batch = []
            batch_size = 10000
            row_count = 0

            for row_index, row in enumerate(rows):
                if len(row) != len(columns):
                    print(f"Skipping row {row_index+1}: column count mismatch")
                    continue

                record = {}
                for value, column in zip(row, columns):
                    value = value.strip()
                    col_type = column["type"]
                    col_name = column["name"]
                    try:
                        if col_type == DataType.INTEGER:
                            typed_value = int(value)
                            record[col_name] = {"type": "integer", "value": typed_value}
                        else:
                            record[col_name] = {"type": "string", "value": value}
                    except ValueError:
                        print(f"Invalid value for {col_name} ({col_type}): {value}")
                        break
                else:
                    values_batch.append(record)

                    if len(values_batch) >= batch_size:
                        result = self.executor.execute({
                            "type": "BULK_INSERT",
                            "table_name": table_name,
                            "values": values_batch
                        })
                        if isinstance(result, str) and result.startswith("Error"):
                            print(f"Insert error: {result}")
                        else:
                            row_count += len(values_batch)
                            print(f"At {row_count}")
                        values_batch = []
            if values_batch:
                result = self.executor.execute({
                    "type": "BULK_INSERT",
                    "table_name": table_name,
                    "values": values_batch
                })
                if isinstance(result, str) and result.startswith("Error"):
                    print(f"Insert error: {result}")
                else:
                    row_count += len(values_batch)


            print(f"\n COPY completed: {row_count} rows inserted into '{table_name}'")

        except Exception as e:
            print(f"Error during COPY: {str(e)}")
            print(f"Expected syntax: COPY table_name FROM 'filename' [DELIMITER 'delimiter'] [CSV]")

    
    def _print_help(self):
        """Print available commands and their descriptions."""
        print(f"\n{Fore.YELLOW}Available SQL Commands:{Style.RESET_ALL}")
        print(f"  {Fore.GREEN}CREATE TABLE{Style.RESET_ALL} table_name (column_name type [PRIMARY KEY], ...)")
        print(f"  {Fore.GREEN}DROP TABLE{Style.RESET_ALL} table_name")
        print(f"  {Fore.GREEN}CREATE INDEX ON{Style.RESET_ALL} table_name (column_name)")
        print(f"  {Fore.GREEN}DROP INDEX ON{Style.RESET_ALL} table_name (column_name)")
        print(f"  {Fore.GREEN}SELECT{Style.RESET_ALL} columns {Fore.GREEN}FROM{Style.RESET_ALL} table [{Fore.GREEN}WHERE{Style.RESET_ALL} conditions] [{Fore.GREEN}ORDER BY{Style.RESET_ALL} columns] [{Fore.GREEN}HAVING{Style.RESET_ALL} condition]")
        print(f"  {Fore.GREEN}INSERT INTO{Style.RESET_ALL} table_name {Fore.GREEN}VALUES{Style.RESET_ALL} (value1, value2, ...)")
        print(f"  {Fore.GREEN}UPDATE{Style.RESET_ALL} table_name {Fore.GREEN}SET{Style.RESET_ALL} column=value [{Fore.GREEN}WHERE{Style.RESET_ALL} conditions]")
        print(f"  {Fore.GREEN}DELETE FROM{Style.RESET_ALL} table_name [{Fore.GREEN}WHERE{Style.RESET_ALL} conditions]")
        
        print(f"\n{Fore.YELLOW}Special Commands:{Style.RESET_ALL}")
        print(f"  {Fore.CYAN}run{Style.RESET_ALL} <file_path> - Execute SQL statements from a file")
        print(f"  {Fore.CYAN}tables{Style.RESET_ALL} - List all available tables and their columns")
        print(f"  {Fore.CYAN}copy{Style.RESET_ALL} table_name {Fore.CYAN}from{Style.RESET_ALL} 'filename' [{Fore.CYAN}delimiter{Style.RESET_ALL} 'char'] [{Fore.CYAN}csv{Style.RESET_ALL}] - Bulk import data")
        print(f"  {Fore.CYAN}exit{Style.RESET_ALL}/{Fore.CYAN}quit{Style.RESET_ALL} - Exit the application")
        print(f"  {Fore.CYAN}help{Style.RESET_ALL} - Show this help message\n")
        
        print(f"{Fore.YELLOW}Multiline Input:{Style.RESET_ALL}")
        print(f"  Type SQL statements spanning multiple lines. Continue typing after the '...>' prompt.")
        print(f"  Submit the query by either:")
        print(f"   - Ending a line with a semicolon ';'")
        print(f"   - Entering an empty line\n")

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='MiniDBMS - DBMS Implementation Project')
    parser.add_argument('--db-dir', default='./database', 
                        help='Directory to store database files')
    parser.add_argument('--script', help='SQL script file to execute')
    parser.add_argument('--demo', action='store_true', 
                        help='Load demo data and run sample queries')
    parser.add_argument('--test-parser', action='store_true',
                        help='Test parser initialization only and exit')
    
    return parser.parse_args()

def load_demo_data(app):
    """Load demo data for quick testing."""
    print(f"{Fore.CYAN}Loading demo data...{Style.RESET_ALL}")
    
    # Demo SQL statements
    demo_sql = [
        # Create tables
        "CREATE TABLE students (id INTEGER PRIMARY KEY, name STRING, age INTEGER)",
        "CREATE TABLE courses (id INTEGER PRIMARY KEY, title STRING, credits INTEGER)",
        "CREATE TABLE enrollments (student_id INTEGER, course_id INTEGER, grade STRING)",
        
        # Insert data
        "INSERT INTO students VALUES (1, 'John Doe', 20)",
        "INSERT INTO students VALUES (2, 'Jane Smith', 22)",
        "INSERT INTO students VALUES (3, 'Bob Johnson', 19)",
        
        "INSERT INTO courses VALUES (101, 'Introduction to Databases', 3)",
        "INSERT INTO courses VALUES (102, 'Data Structures', 4)",
        "INSERT INTO courses VALUES (103, 'Algorithm Analysis', 3)",
        
        "INSERT INTO enrollments VALUES (1, 101, 'A')",
        "INSERT INTO enrollments VALUES (1, 102, 'B')",
        "INSERT INTO enrollments VALUES (2, 101, 'A')",
        "INSERT INTO enrollments VALUES (2, 103, 'A')",
        "INSERT INTO enrollments VALUES (3, 102, 'C')",
    ]
    
    # Execute demo SQL
    for sql in demo_sql:
        print(f"{Fore.CYAN}Executing:{Style.RESET_ALL} {sql}")
        result, _ = app.run_query(sql)
        print(f"{Fore.GREEN}Result:{Style.RESET_ALL} {result}")
    
    print(f"\n{Fore.CYAN}Demo data loaded successfully!{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Try these sample queries:{Style.RESET_ALL}")
    print(f"  {Fore.GREEN}SELECT * FROM students{Style.RESET_ALL}")
    print(f"  {Fore.GREEN}SELECT * FROM students WHERE age > 20{Style.RESET_ALL}")
    print(f"  {Fore.GREEN}SELECT s.name, c.title, e.grade FROM students s, courses c, enrollments e WHERE s.id = e.student_id AND c.id = e.course_id{Style.RESET_ALL}")
    print()

if __name__ == "__main__":
    args = parse_args()
    
    if args.test_parser:
        print("Testing parser initialization...")
        try:
            from parser.sql_parser import SQLParser
            parser = SQLParser()
            print("Parser initialized successfully!")
            sys.exit(0)
        except Exception as e:
            print(f"Parser initialization failed: {str(e)}")
            sys.exit(1)
    
    app = DBMSApplication(db_directory=args.db_dir)
    
    if args.demo:
        load_demo_data(app)
    
    if args.script:
        app.run_script(args.script)
    else:
        app.start_cli()
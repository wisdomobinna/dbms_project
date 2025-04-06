"""
SQL Parser Module

This module handles parsing SQL queries into structured representations.
It implements a basic recursive descent parser for the subset of SQL required
by the project specifications.
"""

import re
from common.exceptions import ParseError, ValidationError
from common.types import DataType

class SQLParser:
    """
    SQL Parser class that converts SQL strings into structured representations.
    """
    
    def __init__(self):
        """Initialize the SQL parser."""
        self.query_types = {
            "CREATE TABLE": self._parse_create_table,
            "DROP TABLE": self._parse_drop_table,
            "CREATE INDEX": self._parse_create_index,
            "DROP INDEX": self._parse_drop_index,
            "SELECT": self._parse_select,
            "INSERT": self._parse_insert,
            "UPDATE": self._parse_update,
            "DELETE": self._parse_delete
        }
    
    def parse(self, query):
        """
        Parse an SQL query and return a structured representation.
        
        Args:
            query (str): The SQL query to parse
            
        Returns:
            dict: A structured representation of the query
            
        Raises:
            ParseError: If the query cannot be parsed
        """
        query = query.strip()
        
        # Determine the query type
        for query_type, parse_func in self.query_types.items():
            if query.upper().startswith(query_type):
                return parse_func(query)
        
        raise ParseError(f"Unsupported SQL statement: {query}")
    
    def validate(self, parsed_query, schema_manager):
        """
        Validate a parsed query against the database schema.
        
        Args:
            parsed_query (dict): The parsed query
            schema_manager: The schema manager for validation
            
        Raises:
            ValidationError: If the query is invalid
        """
        query_type = parsed_query["type"]
        
        if query_type == "CREATE_TABLE":
            self._validate_create_table(parsed_query, schema_manager)
        elif query_type == "DROP_TABLE":
            self._validate_drop_table(parsed_query, schema_manager)
        elif query_type == "CREATE_INDEX":
            self._validate_create_index(parsed_query, schema_manager)
        elif query_type == "DROP_INDEX":
            self._validate_drop_index(parsed_query, schema_manager)
        elif query_type == "SELECT":
            self._validate_select(parsed_query, schema_manager)
        elif query_type == "INSERT":
            self._validate_insert(parsed_query, schema_manager)
        elif query_type == "UPDATE":
            self._validate_update(parsed_query, schema_manager)
        elif query_type == "DELETE":
            self._validate_delete(parsed_query, schema_manager)
    
    def _parse_create_table(self, query):
        """
        Parse a CREATE TABLE statement.
        
        Example: CREATE TABLE students (id INTEGER PRIMARY KEY, name STRING)
        """
        # Match the table name and column definitions
        pattern = r"CREATE\s+TABLE\s+(\w+)\s*\((.*)\)"
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid CREATE TABLE syntax")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        
        # Parse column definitions
        columns = []
        primary_key = None
        foreign_keys = {}
        
        for col_def in self._split_by_comma(columns_str):
            col_def = col_def.strip()
            
            # Check if it's a foreign key definition
            if col_def.upper().startswith("FOREIGN KEY"):
                fk_pattern = r"FOREIGN\s+KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)\s*\((\w+)\)"
                fk_match = re.match(fk_pattern, col_def, re.IGNORECASE)
                
                if not fk_match:
                    raise ParseError(f"Invalid foreign key syntax: {col_def}")
                
                local_col = fk_match.group(1)
                ref_table = fk_match.group(2)
                ref_col = fk_match.group(3)
                
                foreign_keys[local_col] = {
                    "table": ref_table,
                    "column": ref_col
                }
                continue
            
            # Regular column definition
            parts = col_def.split()
            if len(parts) < 2:
                raise ParseError(f"Invalid column definition: {col_def}")
            
            col_name = parts[0]
            col_type = parts[1].upper()
            
            # Check if it's a primary key
            is_primary_key = "PRIMARY KEY" in col_def.upper()
            if is_primary_key and primary_key is not None:
                raise ParseError("Multiple primary keys defined")
            
            if is_primary_key:
                primary_key = col_name
            
            # Validate data type
            if col_type not in ("INTEGER", "STRING"):
                raise ParseError(f"Unsupported data type: {col_type}")
            
            columns.append({
                "name": col_name,
                "type": DataType.INTEGER if col_type == "INTEGER" else DataType.STRING,
                "primary_key": is_primary_key
            })
        
        return {
            "type": "CREATE_TABLE",
            "table_name": table_name,
            "columns": columns,
            "primary_key": primary_key,
            "foreign_keys": foreign_keys
        }
    
    def _parse_drop_table(self, query):
        """
        Parse a DROP TABLE statement.
        
        Example: DROP TABLE students
        """
        pattern = r"DROP\s+TABLE\s+(\w+)"
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid DROP TABLE syntax")
        
        return {
            "type": "DROP_TABLE",
            "table_name": match.group(1)
        }
    
    def _parse_create_index(self, query):
        """
        Parse a CREATE INDEX statement.
        
        Example: CREATE INDEX ON students (id)
        """
        pattern = r"CREATE\s+INDEX\s+ON\s+(\w+)\s*\((\w+)\)"
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid CREATE INDEX syntax")
        
        return {
            "type": "CREATE_INDEX",
            "table_name": match.group(1),
            "column_name": match.group(2)
        }
    
    def _parse_drop_index(self, query):
        """
        Parse a DROP INDEX statement.
        
        Example: DROP INDEX ON students (id)
        """
        pattern = r"DROP\s+INDEX\s+ON\s+(\w+)\s*\((\w+)\)"
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid DROP INDEX syntax")
        
        return {
            "type": "DROP_INDEX",
            "table_name": match.group(1),
            "column_name": match.group(2)
        }
    
    def _parse_select(self, query):
        """
        Parse a SELECT statement.
        
        Examples:
        - SELECT * FROM students
        - SELECT id, name FROM students WHERE age > 18
        - SELECT id, name FROM students WHERE age > 18 AND grade = 'A'
        - SELECT id, name FROM students JOIN grades ON students.id = grades.student_id
        - SELECT MAX(age) FROM students GROUP BY grade HAVING COUNT(*) > 5
        """
        # Basic SELECT pattern
        pattern = r"SELECT\s+(.*?)\s+FROM\s+(.*?)(?:\s+WHERE\s+(.*?))?(?:\s+ORDER\s+BY\s+(.*?))?(?:\s+HAVING\s+(.*?))?$"
        match = re.match(pattern, query, re.IGNORECASE | re.DOTALL)
        
        if not match:
            # Try matching a join query
            join_pattern = r"SELECT\s+(.*?)\s+FROM\s+(.*?)\s+JOIN\s+(.*?)\s+ON\s+(.*?)(?:\s+WHERE\s+(.*?))?(?:\s+ORDER\s+BY\s+(.*?))?(?:\s+HAVING\s+(.*?))?$"
            join_match = re.match(join_pattern, query, re.IGNORECASE | re.DOTALL)
            
            if not join_match:
                raise ParseError("Invalid SELECT syntax")
            
            # Parse join query
            projection = join_match.group(1).strip()
            table1 = join_match.group(2).strip()
            table2 = join_match.group(3).strip()
            join_condition = join_match.group(4).strip()
            where_clause = join_match.group(5).strip() if join_match.group(5) else None
            order_by = join_match.group(6).strip() if join_match.group(6) else None
            having = join_match.group(7).strip() if join_match.group(7) else None
            
            return {
                "type": "SELECT",
                "projection": self._parse_projection(projection),
                "table": table1,
                "join": {
                    "table": table2,
                    "condition": self._parse_join_condition(join_condition)
                },
                "where": self._parse_conditions(where_clause) if where_clause else None,
                "order_by": self._parse_order_by(order_by) if order_by else None,
                "having": self._parse_having(having) if having else None
            }
        
        # Parse basic SELECT query
        projection = match.group(1).strip()
        table = match.group(2).strip()
        where_clause = match.group(3).strip() if match.group(3) else None
        order_by = match.group(4).strip() if match.group(4) else None
        having = match.group(5).strip() if match.group(5) else None
        
        return {
            "type": "SELECT",
            "projection": self._parse_projection(projection),
            "table": table,
            "where": self._parse_conditions(where_clause) if where_clause else None,
            "order_by": self._parse_order_by(order_by) if order_by else None,
            "having": self._parse_having(having) if having else None
        }
    
    def _parse_insert(self, query):
        """
        Parse an INSERT statement.
        
        Example: INSERT INTO students VALUES (1, 'John Doe')
        """
        pattern = r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*)\)"
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid INSERT syntax")
        
        table_name = match.group(1)
        values_str = match.group(2)
        
        # Parse values, handling quoted strings
        values = []
        for value in self._split_by_comma(values_str):
            value = value.strip()
            
            # Handle string literals (quoted)
            if (value.startswith("'") and value.endswith("'")) or \
               (value.startswith('"') and value.endswith('"')):
                values.append({"type": "string", "value": value[1:-1]})
            else:
                try:
                    # Try to parse as integer
                    int_value = int(value)
                    values.append({"type": "integer", "value": int_value})
                except ValueError:
                    raise ParseError(f"Invalid value: {value}")
        
        return {
            "type": "INSERT",
            "table_name": table_name,
            "values": values
        }
    
    def _parse_update(self, query):
        """
        Parse an UPDATE statement.
        
        Example: UPDATE students SET grade = 'A' WHERE id = 1
        """
        pattern = r"UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$"
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid UPDATE syntax")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        
        # Parse SET clause
        set_items = []
        for item in self._split_by_comma(set_clause):
            item = item.strip()
            parts = item.split('=', 1)
            
            if len(parts) != 2:
                raise ParseError(f"Invalid SET item: {item}")
            
            column = parts[0].strip()
            value = parts[1].strip()
            
            # Handle string literals (quoted)
            if (value.startswith("'") and value.endswith("'")) or \
               (value.startswith('"') and value.endswith('"')):
                set_items.append({
                    "column": column,
                    "value": {"type": "string", "value": value[1:-1]}
                })
            else:
                try:
                    # Try to parse as integer
                    int_value = int(value)
                    set_items.append({
                        "column": column,
                        "value": {"type": "integer", "value": int_value}
                    })
                except ValueError:
                    raise ParseError(f"Invalid value: {value}")
        
        return {
            "type": "UPDATE",
            "table_name": table_name,
            "set_items": set_items,
            "where": self._parse_conditions(where_clause) if where_clause else None
        }
    
    def _parse_delete(self, query):
        """
        Parse a DELETE statement.
        
        Example: DELETE FROM students WHERE id = 1
        """
        pattern = r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?$"
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid DELETE syntax")
        
        table_name = match.group(1)
        where_clause = match.group(2)
        
        return {
            "type": "DELETE",
            "table_name": table_name,
            "where": self._parse_conditions(where_clause) if where_clause else None
        }
    
    def _parse_projection(self, projection):
        """Parse the projection part of a SELECT query."""
        if projection == '*':
            return {"type": "all"}
        
        columns = []
        for col in self._split_by_comma(projection):
            col = col.strip()
            
            # Check for aggregation functions
            agg_match = re.match(r"(MIN|MAX|SUM|AVG|COUNT)\((.*?)\)", col, re.IGNORECASE)
            if agg_match:
                func = agg_match.group(1).upper()
                arg = agg_match.group(2).strip()
                columns.append({
                    "type": "aggregation",
                    "function": func,
                    "argument": arg
                })
            else:
                columns.append({
                    "type": "column",
                    "name": col
                })
        
        return {"type": "columns", "columns": columns}
    
    def _parse_conditions(self, conditions_str):
        """Parse WHERE conditions."""
        if not conditions_str:
            return None
        
        # Check for AND/OR operators
        if " AND " in conditions_str.upper():
            parts = conditions_str.split(" AND ", 1)
            return {
                "type": "and",
                "left": self._parse_conditions(parts[0].strip()),
                "right": self._parse_conditions(parts[1].strip())
            }
        elif " OR " in conditions_str.upper():
            parts = conditions_str.split(" OR ", 1)
            return {
                "type": "or",
                "left": self._parse_conditions(parts[0].strip()),
                "right": self._parse_conditions(parts[1].strip())
            }
        
        # Single condition
        operators = ["=", "!=", "<>", "<", "<=", ">", ">="]
        for op in operators:
            if op in conditions_str:
                parts = conditions_str.split(op, 1)
                left = parts[0].strip()
                right = parts[1].strip()
                
                # Handle string literals
                if (right.startswith("'") and right.endswith("'")) or \
                   (right.startswith('"') and right.endswith('"')):
                    right_val = {"type": "string", "value": right[1:-1]}
                else:
                    try:
                        # Try to parse as integer
                        int_value = int(right)
                        right_val = {"type": "integer", "value": int_value}
                    except ValueError:
                        # Must be a column reference
                        right_val = {"type": "column", "name": right}
                
                return {
                    "type": "comparison",
                    "left": {"type": "column", "name": left},
                    "operator": op,
                    "right": right_val
                }
        
        raise ParseError(f"Invalid condition: {conditions_str}")
    
    def _parse_join_condition(self, condition):
        """Parse a JOIN ON condition."""
        if "=" not in condition:
            raise ParseError(f"Invalid join condition: {condition}")
        
        parts = condition.split("=", 1)
        left = parts[0].strip()
        right = parts[1].strip()
        
        # Both sides should be column references with table names
        if "." not in left or "." not in right:
            raise ParseError(f"Join condition must reference columns with table names: {condition}")
        
        left_parts = left.split(".", 1)
        right_parts = right.split(".", 1)
        
        return {
            "left_table": left_parts[0],
            "left_column": left_parts[1],
            "right_table": right_parts[0],
            "right_column": right_parts[1]
        }
    
    def _parse_order_by(self, order_by):
        """Parse ORDER BY clause."""
        columns = []
        for col in self._split_by_comma(order_by):
            col = col.strip()
            
            if col.upper().endswith(" DESC"):
                columns.append({
                    "column": col[:-5].strip(),
                    "direction": "DESC"
                })
            elif col.upper().endswith(" ASC"):
                columns.append({
                    "column": col[:-4].strip(),
                    "direction": "ASC"
                })
            else:
                columns.append({
                    "column": col,
                    "direction": "ASC"  # Default is ascending
                })
        
        return columns
    
    def _parse_having(self, having):
        """Parse HAVING clause."""
        # HAVING clause is similar to WHERE but typically contains aggregations
        return self._parse_conditions(having)
    
    def _split_by_comma(self, text):
        """
        Split a string by commas, accounting for parentheses and quotes.
        """
        result = []
        current = ""
        in_quotes = False
        quote_char = None
        paren_level = 0
        
        for char in text:
            if char in ["'", '"'] and (not quote_char or char == quote_char):
                in_quotes = not in_quotes
                if in_quotes:
                    quote_char = char
                else:
                    quote_char = None
                current += char
            elif char == '(' and not in_quotes:
                paren_level += 1
                current += char
            elif char == ')' and not in_quotes:
                paren_level -= 1
                current += char
            elif char == ',' and not in_quotes and paren_level == 0:
                result.append(current)
                current = ""
            else:
                current += char
        
        if current:
            result.append(current)
        
        return result
    
    # Validation methods
    def _validate_create_table(self, parsed_query, schema_manager):
        """Validate CREATE TABLE query."""
        table_name = parsed_query["table_name"]
        
        if schema_manager.table_exists(table_name):
            raise ValidationError(f"Table '{table_name}' already exists")
    
    def _validate_drop_table(self, parsed_query, schema_manager):
        """Validate DROP TABLE query."""
        table_name = parsed_query["table_name"]
        
        if not schema_manager.table_exists(table_name):
            raise ValidationError(f"Table '{table_name}' does not exist")
    
    def _validate_create_index(self, parsed_query, schema_manager):
        """Validate CREATE INDEX query."""
        table_name = parsed_query["table_name"]
        column_name = parsed_query["column_name"]
        
        if not schema_manager.table_exists(table_name):
            raise ValidationError(f"Table '{table_name}' does not exist")
        
        if not schema_manager.column_exists(table_name, column_name):
            raise ValidationError(f"Column '{column_name}' does not exist in table '{table_name}'")
        
        if schema_manager.index_exists(table_name, column_name):
            raise ValidationError(f"Index already exists on '{table_name}.{column_name}'")
    
    def _validate_drop_index(self, parsed_query, schema_manager):
        """Validate DROP INDEX query."""
        table_name = parsed_query["table_name"]
        column_name = parsed_query["column_name"]
        
        if not schema_manager.table_exists(table_name):
            raise ValidationError(f"Table '{table_name}' does not exist")
        
        if not schema_manager.index_exists(table_name, column_name):
            raise ValidationError(f"No index exists on '{table_name}.{column_name}'")
    
    def _validate_select(self, parsed_query, schema_manager):
        """Validate SELECT query."""
        table_name = parsed_query["table"]
        
        if not schema_manager.table_exists(table_name):
            raise ValidationError(f"Table '{table_name}' does not exist")
        
        # Validate projection
        projection = parsed_query["projection"]
        if projection["type"] == "columns":
            for col in projection["columns"]:
                if col["type"] == "column" and not schema_manager.column_exists(table_name, col["name"]):
                    raise ValidationError(f"Column '{col['name']}' does not exist in table '{table_name}'")
        
        # Validate join if present
        if "join" in parsed_query and parsed_query["join"]:
            join_table = parsed_query["join"]["table"]
            if not schema_manager.table_exists(join_table):
                raise ValidationError(f"Join table '{join_table}' does not exist")
            
            join_cond = parsed_query["join"]["condition"]
            if not schema_manager.column_exists(join_cond["left_table"], join_cond["left_column"]):
                raise ValidationError(f"Column '{join_cond['left_column']}' does not exist in table '{join_cond['left_table']}'")
            
            if not schema_manager.column_exists(join_cond["right_table"], join_cond["right_column"]):
                raise ValidationError(f"Column '{join_cond['right_column']}' does not exist in table '{join_cond['right_table']}'")
    
    def _validate_insert(self, parsed_query, schema_manager):
        """Validate INSERT query."""
        table_name = parsed_query["table_name"]
        
        if not schema_manager.table_exists(table_name):
            raise ValidationError(f"Table '{table_name}' does not exist")
        
        # Check number of values matches number of columns
        columns = schema_manager.get_columns(table_name)
        if len(parsed_query["values"]) != len(columns):
            raise ValidationError(f"INSERT has {len(parsed_query['values'])} values but table '{table_name}' has {len(columns)} columns")
        
        # Check data types
        for i, (value, column) in enumerate(zip(parsed_query["values"], columns)):
            if value["type"] == "integer" and column["type"] != DataType.INTEGER:
                raise ValidationError(f"Type mismatch for column {i+1}: expected STRING, got INTEGER")
            elif value["type"] == "string" and column["type"] != DataType.STRING:
                raise ValidationError(f"Type mismatch for column {i+1}: expected INTEGER, got STRING")
        
        # Check primary key constraint if applicable
        primary_key = schema_manager.get_primary_key(table_name)
        if primary_key:
            pk_index = next(i for i, col in enumerate(columns) if col["name"] == primary_key)
            pk_value = parsed_query["values"][pk_index]["value"]
            
            if schema_manager.primary_key_exists(table_name, pk_value):
                raise ValidationError(f"Duplicate primary key value: {pk_value}")
        
        # Check foreign key constraints if applicable
        foreign_keys = schema_manager.get_foreign_keys(table_name)
        for fk_column, fk_ref in foreign_keys.items():
            fk_index = next(i for i, col in enumerate(columns) if col["name"] == fk_column)
            fk_value = parsed_query["values"][fk_index]["value"]
            
            if not schema_manager.foreign_key_exists(fk_ref["table"], fk_ref["column"], fk_value):
                raise ValidationError(f"Foreign key constraint violation: {fk_value} does not exist in {fk_ref['table']}.{fk_ref['column']}")
    
    def _validate_update(self, parsed_query, schema_manager):
        """Validate UPDATE query."""
        table_name = parsed_query["table_name"]
        
        if not schema_manager.table_exists(table_name):
            raise ValidationError(f"Table '{table_name}' does not exist")
        
        # Validate columns being updated
        for set_item in parsed_query["set_items"]:
            column_name = set_item["column"]
            if not schema_manager.column_exists(table_name, column_name):
                raise ValidationError(f"Column '{column_name}' does not exist in table '{table_name}'")
            
            # Check type compatibility
            column = schema_manager.get_column(table_name, column_name)
            value = set_item["value"]
            
            if value["type"] == "integer" and column["type"] != DataType.INTEGER:
                raise ValidationError(f"Type mismatch for column '{column_name}': expected STRING, got INTEGER")
            elif value["type"] == "string" and column["type"] != DataType.STRING:
                raise ValidationError(f"Type mismatch for column '{column_name}': expected INTEGER, got STRING")
        
        # Validate WHERE clause if present
        if parsed_query["where"]:
            self._validate_condition(parsed_query["where"], table_name, schema_manager)
    
    def _validate_delete(self, parsed_query, schema_manager):
        """Validate DELETE query."""
        table_name = parsed_query["table_name"]
        
        if not schema_manager.table_exists(table_name):
            raise ValidationError(f"Table '{table_name}' does not exist")
        
        # Validate WHERE clause if present
        if parsed_query["where"]:
            self._validate_condition(parsed_query["where"], table_name, schema_manager)
    
    def _validate_condition(self, condition, table_name, schema_manager):
        """Validate a WHERE condition."""
        if condition["type"] == "and" or condition["type"] == "or":
            self._validate_condition(condition["left"], table_name, schema_manager)
            self._validate_condition(condition["right"], table_name, schema_manager)
        elif condition["type"] == "comparison":
            # Validate left side (column)
            if condition["left"]["type"] == "column":
                column_name = condition["left"]["name"]
                if not schema_manager.column_exists(table_name, column_name):
                    raise ValidationError(f"Column '{column_name}' does not exist in table '{table_name}'")
            
            # Validate right side (if it's a column)
            if condition["right"]["type"] == "column":
                column_name = condition["right"]["name"]
                if not schema_manager.column_exists(table_name, column_name):
                    raise ValidationError(f"Column '{column_name}' does not exist in table '{table_name}'")
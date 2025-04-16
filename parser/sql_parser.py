"""
SQL Parser Module using PLY

This module handles parsing SQL queries into structured representations
using the PLY (Python Lex-Yacc) library for the DBMS implementation project.
"""

import os
import ply.lex as lex
import ply.yacc as yacc
from common.exceptions import ParseError, ValidationError
from common.types import DataType

class SQLParser:
    """
    SQL Parser class that converts SQL strings into structured representations
    using PLY for lexical analysis and parsing.
    """
    
    # List of token names
    tokens = (
        'CREATE', 'DROP', 'TABLE', 'INDEX', 'ON', 'SELECT', 'FROM', 'WHERE',
        'INSERT', 'INTO', 'VALUES', 'DELETE', 'UPDATE', 'SET', 'ORDER', 'BY',
        'HAVING', 'JOIN', 'ASC', 'DESC', 'AND', 'OR', 'INTEGER', 'STRING',
        'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'SHOW', 'TABLES', 'DESCRIBE',
        'IDENTIFIER', 'NUMBER', 'STRING_LITERAL', 'COMMA', 'SEMICOLON',
        'LPAREN', 'RPAREN', 'DOT', 'EQUALS', 'NOTEQUALS', 'LT', 'GT', 'LE', 'GE',
        'ASTERISK'
    )
    
    # Regular expressions for simple tokens
    t_COMMA = r','
    t_SEMICOLON = r';'
    t_LPAREN = r'\('
    t_RPAREN = r'\)'
    t_DOT = r'\.'
    t_EQUALS = r'='
    t_NOTEQUALS = r'!=|<>'
    t_LT = r'<'
    t_GT = r'>'
    t_LE = r'<='
    t_GE = r'>='
    t_ASTERISK = r'\*'
    
    # Reserved words
    reserved = {
        'create': 'CREATE',
        'drop': 'DROP',
        'table': 'TABLE',
        'index': 'INDEX',
        'on': 'ON',
        'select': 'SELECT',
        'from': 'FROM',
        'where': 'WHERE',
        'insert': 'INSERT',
        'into': 'INTO',
        'values': 'VALUES',
        'delete': 'DELETE',
        'update': 'UPDATE',
        'set': 'SET',
        'order': 'ORDER',
        'by': 'BY',
        'having': 'HAVING',
        'join': 'JOIN',
        'asc': 'ASC',
        'desc': 'DESC',
        'and': 'AND',
        'or': 'OR',
        'integer': 'INTEGER',
        'string': 'STRING',
        'primary': 'PRIMARY',
        'key': 'KEY',
        'foreign': 'FOREIGN',
        'references': 'REFERENCES',
        'show': 'SHOW',
        'tables': 'TABLES',
        'describe': 'DESCRIBE'
    }
    
    def t_IDENTIFIER(self, t):
        r'[a-zA-Z_][a-zA-Z0-9_]*'
        # Check for reserved words
        t.type = self.reserved.get(t.value.lower(), 'IDENTIFIER')
        return t
    
    def t_NUMBER(self, t):
        r'\d+'
        t.value = int(t.value)
        return t
    
    def t_STRING_LITERAL(self, t):
        r"'[^']*'|\"[^\"]*\""
        # Remove the quotes
        t.value = t.value[1:-1]
        return t
    
    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)
    
    # A string containing ignored characters (spaces and tabs)
    t_ignore = ' \t'
    
    # Error handling rule
    def t_error(self, t):
        raise ParseError(f"Illegal character '{t.value[0]}'")
    
    # Build the lexer
    def build_lexer(self):
        # Store parser output directory for PLY
        self.output_dir = os.path.dirname(os.path.abspath(__file__))
        self.lexer = lex.lex(module=self, outputdir=self.output_dir, optimize=0, debug=0)
    
    # Build the parser
    def build_parser(self):
        self.parser = yacc.yacc(module=self, outputdir=self.output_dir, optimize=0, debug=0)
    
    def __init__(self):
        """Initialize the SQL parser."""
        self.build_lexer()
        self.build_parser()
    
    # Define grammar rules
    def p_statement(self, p):
        '''statement : create_table_statement
                     | drop_table_statement
                     | create_index_statement
                     | drop_index_statement
                     | select_statement
                     | insert_statement
                     | update_statement
                     | delete_statement
                     | show_tables_statement
                     | describe_statement'''
        p[0] = p[1]
    
    def p_show_tables_statement(self, p):
        'show_tables_statement : SHOW TABLES'
        p[0] = {'type': 'SHOW_TABLES'}
    
    def p_describe_statement(self, p):
        'describe_statement : DESCRIBE IDENTIFIER'
        p[0] = {'type': 'DESCRIBE', 'table_name': p[2]}
    
    def p_create_table_statement(self, p):
        'create_table_statement : CREATE TABLE IDENTIFIER LPAREN column_def_list RPAREN'
        columns = []
        primary_key = None
        foreign_keys = {}
        
        # Process column definitions
        for col_def in p[5]:
            if col_def.get('type') == 'column':
                columns.append({
                    'name': col_def['name'],
                    'type': col_def['data_type'],
                    'primary_key': col_def.get('primary_key', False)
                })
                if col_def.get('primary_key'):
                    primary_key = col_def['name']
            elif col_def.get('type') == 'foreign_key':
                foreign_keys[col_def['column']] = {
                    'table': col_def['ref_table'],
                    'column': col_def['ref_column']
                }
        
        p[0] = {
            'type': 'CREATE_TABLE',
            'table_name': p[3],
            'columns': columns,
            'primary_key': primary_key,
            'foreign_keys': foreign_keys
        }
    
    def p_column_def_list(self, p):
        '''column_def_list : column_def
                          | column_def COMMA column_def_list'''
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = [p[1]] + p[3]
    
    def p_column_def(self, p):
        '''column_def : IDENTIFIER INTEGER primary_key_opt
                      | IDENTIFIER STRING primary_key_opt
                      | foreign_key_def'''
        if len(p) == 4:  # Regular column definition
            p[0] = {
                'type': 'column',
                'name': p[1],
                'data_type': DataType.INTEGER if p[2].lower() == 'integer' else DataType.STRING,
                'primary_key': p[3]
            }
        else:  # Foreign key definition
            p[0] = p[1]
    
    def p_primary_key_opt(self, p):
        '''primary_key_opt : PRIMARY KEY
                          | '''
        if len(p) == 3:
            p[0] = True
        else:
            p[0] = False
    
    def p_foreign_key_def(self, p):
        'foreign_key_def : FOREIGN KEY LPAREN IDENTIFIER RPAREN REFERENCES IDENTIFIER LPAREN IDENTIFIER RPAREN'
        p[0] = {
            'type': 'foreign_key',
            'column': p[4],
            'ref_table': p[7],
            'ref_column': p[9]
        }
    
    def p_drop_table_statement(self, p):
        'drop_table_statement : DROP TABLE IDENTIFIER'
        p[0] = {
            'type': 'DROP_TABLE',
            'table_name': p[3]
        }
    
    def p_create_index_statement(self, p):
        'create_index_statement : CREATE INDEX ON IDENTIFIER LPAREN IDENTIFIER RPAREN'
        p[0] = {
            'type': 'CREATE_INDEX',
            'table_name': p[4],
            'column_name': p[6]
        }
    
    def p_drop_index_statement(self, p):
        'drop_index_statement : DROP INDEX ON IDENTIFIER LPAREN IDENTIFIER RPAREN'
        p[0] = {
            'type': 'DROP_INDEX',
            'table_name': p[4],
            'column_name': p[6]
        }
    
    def p_select_statement(self, p):
        '''select_statement : SELECT select_list FROM table_reference join_clauses_opt where_clause_opt order_by_clause_opt having_clause_opt'''
        p[0] = {
            'type': 'SELECT',
            'projection': p[2],
            'table': p[4],
            'join': p[5],
            'where': p[6],
            'order_by': p[7],
            'having': p[8]
        }
    
    def p_select_list(self, p):
        '''select_list : ASTERISK
                      | column_list'''
        if p[1] == '*':
            p[0] = {'type': 'all'}
        else:
            p[0] = {'type': 'columns', 'columns': p[1]}
    
    def p_column_list(self, p):
        '''column_list : column_item
                       | column_item COMMA column_list'''
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = [p[1]] + p[3]
    
    def p_column_item(self, p):
        '''column_item : IDENTIFIER
                       | IDENTIFIER DOT IDENTIFIER
                       | aggregation_function'''
        if isinstance(p[1], dict):  # Aggregation function
            p[0] = p[1]
        elif len(p) > 2:  # Qualified column (table.column)
            p[0] = {
                'type': 'column',
                'name': f"{p[1]}.{p[3]}"
            }
        else:  # Simple column
            p[0] = {
                'type': 'column',
                'name': p[1]
            }
    
    def p_aggregation_function(self, p):
        'aggregation_function : IDENTIFIER LPAREN IDENTIFIER RPAREN'
        p[0] = {
            'type': 'aggregation',
            'function': p[1].upper(),
            'argument': p[3]
        }
    
    def p_table_reference(self, p):
        'table_reference : IDENTIFIER'
        p[0] = p[1]
    
    def p_join_clauses_opt(self, p):
        '''join_clauses_opt : join_clause
                           | join_clause join_clauses_opt
                           | '''
        if len(p) == 1:  # Empty
            p[0] = None
        elif len(p) == 2:  # Single join
            p[0] = p[1]
        else:  # Multiple joins
            if isinstance(p[1], list):
                p[0] = p[1] + [p[2]]
            else:
                p[0] = [p[1], p[2]]
                
    def p_join_clause(self, p):
        'join_clause : JOIN IDENTIFIER ON join_condition'
        p[0] = {
            'table': p[2],
            'condition': p[4]
        }
    
    def p_join_condition(self, p):
        'join_condition : IDENTIFIER DOT IDENTIFIER EQUALS IDENTIFIER DOT IDENTIFIER'
        p[0] = {
            'left_table': p[1],
            'left_column': p[3],
            'right_table': p[5],
            'right_column': p[7]
        }
    
    def p_where_clause_opt(self, p):
        '''where_clause_opt : WHERE condition
                           | '''
        if len(p) == 3:
            p[0] = p[2]
        else:
            p[0] = None
    
    def p_condition(self, p):
        '''condition : comparison
                     | condition AND condition
                     | condition OR condition
                     | LPAREN condition RPAREN'''
        if len(p) == 2:  # Simple comparison
            p[0] = p[1]
        elif len(p) == 4:
            if p[1] == '(':  # Parenthesized condition
                p[0] = p[2]
            else:  # AND/OR condition
                p[0] = {
                    'type': p[2].lower(),
                    'left': p[1],
                    'right': p[3]
                }
    
    def p_comparison(self, p):
        '''comparison : IDENTIFIER comp_operator expression
                      | IDENTIFIER DOT IDENTIFIER comp_operator expression'''
        if len(p) > 4:  # Qualified column
            p[0] = {
                'type': 'comparison',
                'left': {'type': 'column', 'name': f"{p[1]}.{p[3]}"},
                'operator': p[4],
                'right': p[5]
            }
        else:  # Simple column
            p[0] = {
                'type': 'comparison',
                'left': {'type': 'column', 'name': p[1]},
                'operator': p[2],
                'right': p[3]
            }
    
    def p_comp_operator(self, p):
        '''comp_operator : EQUALS
                        | NOTEQUALS
                        | LT
                        | GT
                        | LE
                        | GE'''
        p[0] = p[1]
    
    def p_expression(self, p):
        '''expression : IDENTIFIER
                     | NUMBER
                     | STRING_LITERAL'''
        if isinstance(p[1], int):
            p[0] = {'type': 'integer', 'value': p[1]}
        elif isinstance(p[1], str) and (p.slice[1].type == 'STRING_LITERAL'):
            p[0] = {'type': 'string', 'value': p[1]}
        else:
            p[0] = {'type': 'column', 'name': p[1]}
    
    def p_order_by_clause_opt(self, p):
        '''order_by_clause_opt : ORDER BY order_list
                              | '''
        if len(p) == 4:
            p[0] = p[3]
        else:
            p[0] = None
    
    def p_order_list(self, p):
        '''order_list : order_item
                     | order_item COMMA order_list'''
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = [p[1]] + p[3]
    
    def p_order_item(self, p):
        '''order_item : IDENTIFIER
                     | IDENTIFIER ASC
                     | IDENTIFIER DESC'''
        if len(p) == 2:
            p[0] = {'column': p[1], 'direction': 'ASC'}
        else:
            p[0] = {'column': p[1], 'direction': p[2]}
    
    def p_having_clause_opt(self, p):
        '''having_clause_opt : HAVING condition
                            | '''
        if len(p) == 3:
            p[0] = p[2]
        else:
            p[0] = None
    
    def p_insert_statement(self, p):
        'insert_statement : INSERT INTO IDENTIFIER VALUES LPAREN value_list RPAREN'
        p[0] = {
            'type': 'INSERT',
            'table_name': p[3],
            'values': p[6]
        }
    
    def p_value_list(self, p):
        '''value_list : expression
                     | expression COMMA value_list'''
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = [p[1]] + p[3]
    
    def p_update_statement(self, p):
        'update_statement : UPDATE IDENTIFIER SET set_list where_clause_opt'
        p[0] = {
            'type': 'UPDATE',
            'table_name': p[2],
            'set_items': p[4],
            'where': p[5]
        }
    
    def p_set_list(self, p):
        '''set_list : set_item
                   | set_item COMMA set_list'''
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = [p[1]] + p[3]
    
    def p_set_item(self, p):
        'set_item : IDENTIFIER EQUALS expression'
        p[0] = {
            'column': p[1],
            'value': p[3]
        }
    
    def p_delete_statement(self, p):
        'delete_statement : DELETE FROM IDENTIFIER where_clause_opt'
        p[0] = {
            'type': 'DELETE',
            'table_name': p[3],
            'where': p[4]
        }
    
    def p_error(self, p):
        if p:
            raise ParseError(f"Syntax error at '{p.value}'")
        else:
            raise ParseError("Syntax error at EOF")
    
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
        try:
            # Remove trailing semicolon if present
            query = query.strip()
            if query.endswith(';'):
                query = query[:-1]
                
            return self.parser.parse(query, lexer=self.lexer)
        except Exception as e:
            if isinstance(e, ParseError):
                raise
            raise ParseError(f"Error parsing query: {str(e)}")
    
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
        elif query_type == "DESCRIBE":
            self._validate_describe(parsed_query, schema_manager)
        # SHOW TABLES doesn't need validation
    
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
    
    def _validate_describe(self, parsed_query, schema_manager):
        """Validate DESCRIBE query."""
        table_name = parsed_query["table_name"]
        
        if not schema_manager.table_exists(table_name):
            raise ValidationError(f"Table '{table_name}' does not exist")
    
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
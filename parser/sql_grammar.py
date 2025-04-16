"""
SQL Grammar definitions for PLY parser.
"""
from common.types import DataType

# Grammar rule definitions
def p_statement(parser, p):
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

def p_create_table_statement(parser, p):
    'create_table_statement : CREATE TABLE ID LPAREN column_def_list RPAREN'
    columns = []
    primary_key = None
    foreign_keys = {}
    
    # Process column definitions
    for col_def in p[5]:
        if col_def.get('primary_key'):
            primary_key = col_def['name']
        
        if 'foreign_key' in col_def:
            foreign_keys[col_def['name']] = col_def['foreign_key']
        
        columns.append({
            'name': col_def['name'],
            'type': col_def['type'],
            'primary_key': col_def.get('primary_key', False)
        })
    
    p[0] = {
        'type': 'CREATE_TABLE',
        'table_name': p[3],
        'columns': columns,
        'primary_key': primary_key,
        'foreign_keys': foreign_keys
    }

def p_column_def_list(parser, p):
    '''column_def_list : column_def
                      | column_def COMMA column_def_list'''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = [p[1]] + p[3]

def p_column_def(parser, p):
    '''column_def : ID INTEGER
                  | ID INTEGER PRIMARY KEY
                  | ID VARCHAR LPAREN NUMBER RPAREN'''
    column_def = {
        'name': p[1],
        'type': DataType.INTEGER if p[2] == 'INTEGER' else DataType.STRING
    }
    
    if len(p) > 3 and p[3] == 'PRIMARY' and p[4] == 'KEY':
        column_def['primary_key'] = True
    
    p[0] = column_def

def p_drop_table_statement(parser, p):
    'drop_table_statement : DROP TABLE ID'
    p[0] = {
        'type': 'DROP_TABLE',
        'table_name': p[3]
    }

def p_create_index_statement(parser, p):
    'create_index_statement : CREATE INDEX ON ID LPAREN ID RPAREN'
    p[0] = {
        'type': 'CREATE_INDEX',
        'table_name': p[4],
        'column_name': p[6]
    }

def p_drop_index_statement(parser, p):
    'drop_index_statement : DROP INDEX ON ID LPAREN ID RPAREN'
    p[0] = {
        'type': 'DROP_INDEX',
        'table_name': p[4],
        'column_name': p[6]
    }

def p_select_statement(parser, p):
    '''select_statement : SELECT projection FROM ID
                        | SELECT projection FROM ID WHERE condition
                        | SELECT projection FROM ID ORDER BY order_list
                        | SELECT projection FROM ID WHERE condition ORDER BY order_list
                        | SELECT projection FROM ID JOIN ID ON join_condition
                        | SELECT projection FROM ID JOIN ID ON join_condition WHERE condition
                        | SELECT projection FROM ID JOIN ID ON join_condition ORDER BY order_list
                        | SELECT projection FROM ID JOIN ID ON join_condition WHERE condition ORDER BY order_list'''
    result = {
        'type': 'SELECT',
        'projection': p[2],
        'table': p[4]
    }
    
    # Check for JOIN clause
    idx = 5
    if len(p) > idx and p[idx] == 'JOIN':
        result['join'] = {
            'table': p[idx+1],
            'condition': p[idx+3]
        }
        idx += 4  # Move past JOIN ID ON join_condition
    
    # Check for WHERE clause
    if idx < len(p) and p[idx] == 'WHERE':
        result['where'] = p[idx+1]
        idx += 2  # Move past WHERE condition
    
    # Check for ORDER BY clause
    if idx < len(p) and p[idx] == 'ORDER' and p[idx+1] == 'BY':
        result['order_by'] = p[idx+2]
    
    p[0] = result

def p_join_condition(parser, p):
    'join_condition : ID DOT ID EQUALS ID DOT ID'
    p[0] = {
        'left_table': p[1],
        'left_column': p[3],
        'right_table': p[5],
        'right_column': p[7]
    }

def p_projection(parser, p):
    '''projection : TIMES
                  | column_list'''
    if p[1] == '*':
        p[0] = {'type': 'all'}
    else:
        p[0] = {'type': 'columns', 'columns': p[1]}

def p_column_list(parser, p):
    '''column_list : ID
                   | ID COMMA column_list'''
    if len(p) == 2:
        p[0] = [{'type': 'column', 'name': p[1]}]
    else:
        p[0] = [{'type': 'column', 'name': p[1]}] + p[3]

def p_condition(parser, p):
    '''condition : ID EQUALS value
                 | ID GT value
                 | ID LT value
                 | ID GE value
                 | ID LE value
                 | ID NE value
                 | condition AND condition
                 | condition OR condition'''
    if p[2] in ('AND', 'OR'):
        p[0] = {
            'type': p[2].lower(),
            'left': p[1],
            'right': p[3]
        }
    else:
        p[0] = {
            'type': 'comparison',
            'left': {'type': 'column', 'name': p[1]},
            'operator': p[2],
            'right': p[3]
        }

def p_value(parser, p):
    '''value : NUMBER
             | STRING
             | ID'''
    if isinstance(p[1], int):
        p[0] = {'type': 'integer', 'value': p[1]}
    elif isinstance(p[1], str) and (p[1].startswith("'") or p[1].startswith('"')):
        p[0] = {'type': 'string', 'value': p[1][1:-1]}
    else:
        p[0] = {'type': 'column', 'name': p[1]}

def p_order_list(parser, p):
    '''order_list : order_item
                  | order_item COMMA order_list'''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = [p[1]] + p[3]

def p_order_item(parser, p):
    '''order_item : ID
                  | ID ASC
                  | ID DESC'''
    if len(p) == 2:
        p[0] = {'column': p[1], 'direction': 'ASC'}
    else:
        p[0] = {'column': p[1], 'direction': p[2]}

def p_insert_statement(parser, p):
    'insert_statement : INSERT INTO ID VALUES LPAREN value_list RPAREN'
    p[0] = {
        'type': 'INSERT',
        'table_name': p[3],
        'values': p[6]
    }

def p_value_list(parser, p):
    '''value_list : value
                  | value COMMA value_list'''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = [p[1]] + p[3]

def p_update_statement(parser, p):
    '''update_statement : UPDATE ID SET set_list
                        | UPDATE ID SET set_list WHERE condition'''
    result = {
        'type': 'UPDATE',
        'table_name': p[2],
        'set_items': p[4]
    }
    
    if len(p) > 5:
        result['where'] = p[6]
    else:
        result['where'] = None
    
    p[0] = result

def p_set_list(parser, p):
    '''set_list : set_item
                | set_item COMMA set_list'''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = [p[1]] + p[3]

def p_set_item(parser, p):
    'set_item : ID EQUALS value'
    p[0] = {
        'column': p[1],
        'value': p[3]
    }

def p_delete_statement(parser, p):
    '''delete_statement : DELETE FROM ID
                        | DELETE FROM ID WHERE condition'''
    result = {
        'type': 'DELETE',
        'table_name': p[3]
    }
    
    if len(p) > 4:
        result['where'] = p[5]
    else:
        result['where'] = None
    
    p[0] = result

def p_show_tables_statement(parser, p):
    'show_tables_statement : SHOW TABLES'
    p[0] = {'type': 'SHOW_TABLES'}

def p_describe_statement(parser, p):
    'describe_statement : DESCRIBE ID'
    p[0] = {
        'type': 'DESCRIBE',
        'table_name': p[2]
    }

def p_error(parser, p):
    if p:
        raise SyntaxError(f"Syntax error at '{p.value}'")
    else:
        raise SyntaxError("Syntax error at EOF")
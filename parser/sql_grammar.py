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
        
        column_def = {
            'name': col_def['name'],
            'type': col_def['type'],
            'primary_key': col_def.get('primary_key', False)
        }
        
        # Include auto_increment attribute if present
        if col_def.get('auto_increment'):
            column_def['auto_increment'] = True
            
        columns.append(column_def)
    
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
                  | ID INTEGER AUTO_INCREMENT
                  | ID INTEGER PRIMARY KEY AUTO_INCREMENT
                  | ID INTEGER AUTO_INCREMENT PRIMARY KEY
                  | ID VARCHAR LPAREN NUMBER RPAREN'''
    column_def = {
        'name': p[1],
        'type': DataType.INTEGER if p[2] == 'INTEGER' else DataType.STRING
    }
    
    # Process options - they can appear in any order
    has_primary_key = False
    has_auto_increment = False
    
    for i in range(3, len(p)):
        if p[i] == 'PRIMARY' and i+1 < len(p) and p[i+1] == 'KEY':
            column_def['primary_key'] = True
            has_primary_key = True
        elif p[i] == 'AUTO_INCREMENT':
            column_def['auto_increment'] = True
            has_auto_increment = True
    
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
    '''select_statement : SELECT projection FROM table_reference
                        | SELECT projection FROM table_reference where_clause
                        | SELECT projection FROM table_reference order_by_clause
                        | SELECT projection FROM table_reference group_by_clause
                        | SELECT projection FROM table_reference limit_clause
                        | SELECT projection FROM table_reference where_clause order_by_clause
                        | SELECT projection FROM table_reference where_clause group_by_clause
                        | SELECT projection FROM table_reference where_clause limit_clause
                        | SELECT projection FROM table_reference order_by_clause limit_clause
                        | SELECT projection FROM table_reference group_by_clause order_by_clause
                        | SELECT projection FROM table_reference group_by_clause limit_clause
                        | SELECT projection FROM table_reference where_clause order_by_clause limit_clause
                        | SELECT projection FROM table_reference where_clause group_by_clause order_by_clause
                        | SELECT projection FROM table_reference where_clause group_by_clause limit_clause
                        | SELECT projection FROM table_reference group_by_clause order_by_clause limit_clause
                        | SELECT projection FROM table_reference where_clause group_by_clause order_by_clause limit_clause
                        | SELECT projection FROM table_reference join_clause
                        | SELECT projection FROM table_reference join_clause where_clause
                        | SELECT projection FROM table_reference join_clause order_by_clause
                        | SELECT projection FROM table_reference join_clause group_by_clause
                        | SELECT projection FROM table_reference join_clause limit_clause
                        | SELECT projection FROM table_reference join_clause where_clause order_by_clause
                        | SELECT projection FROM table_reference join_clause where_clause group_by_clause
                        | SELECT projection FROM table_reference join_clause where_clause limit_clause
                        | SELECT projection FROM table_reference join_clause order_by_clause limit_clause
                        | SELECT projection FROM table_reference join_clause group_by_clause order_by_clause
                        | SELECT projection FROM table_reference join_clause group_by_clause limit_clause
                        | SELECT projection FROM table_reference join_clause where_clause order_by_clause limit_clause
                        | SELECT projection FROM table_reference join_clause where_clause group_by_clause order_by_clause
                        | SELECT projection FROM table_reference join_clause where_clause group_by_clause limit_clause
                        | SELECT projection FROM table_reference join_clause group_by_clause order_by_clause limit_clause
                        | SELECT projection FROM table_reference join_clause where_clause group_by_clause order_by_clause limit_clause'''
    result = {
        'type': 'SELECT',
        'projection': p[2],
        'table': p[4]
    }
    
    # Process remaining clauses 
    i = 5
    while i < len(p):
        if p[i] == 'JOIN':
            # Process JOIN clause
            result['join'] = {
                'table': p[i+1],
                'condition': p[i+3]
            }
            i += 4  # Move past JOIN ID ON join_condition
        elif p[i] == 'WHERE':
            # Process WHERE clause
            result['where'] = p[i+1]
            i += 2
        elif p[i] == 'ORDER' and i+1 < len(p) and p[i+1] == 'BY':
            # Process ORDER BY clause
            result['order_by'] = p[i+2]
            i += 3
        elif p[i] == 'GROUP' and i+1 < len(p) and p[i+1] == 'BY':
            # Process GROUP BY clause
            result['group_by'] = p[i+2]
            i += 3
        elif p[i] == 'HAVING':
            # Process HAVING clause
            result['having'] = p[i+1]
            i += 2
        elif p[i] == 'LIMIT':
            # Process LIMIT clause
            result['limit'] = p[i+1]
            i += 2
            # Check if there's an OFFSET
            if i < len(p) and p[i] == 'OFFSET':
                result['offset'] = p[i+1]
                i += 2
        else:
            # Unknown clause, skip it
            i += 1
    
    p[0] = result

def p_table_reference(parser, p):
    '''table_reference : ID
                       | ID ID
                       | ID AS ID
                       | LPAREN subquery RPAREN ID
                       | LPAREN subquery RPAREN AS ID'''
    if len(p) == 2:
        p[0] = p[1]  # Simple table reference
    elif len(p) == 3:
        p[0] = {'name': p[1], 'alias': p[2]}  # Table with alias (implicit)
    elif len(p) == 4:
        p[0] = {'name': p[1], 'alias': p[3]}  # Table with alias (explicit)
    elif len(p) == 5:
        # Derived table: (subquery) alias
        p[0] = {'type': 'derived_table', 'subquery': p[2], 'alias': p[4]}
    else:
        # Derived table with explicit AS: (subquery) AS alias
        p[0] = {'type': 'derived_table', 'subquery': p[2], 'alias': p[5]}

def p_where_clause(parser, p):
    'where_clause : WHERE condition'
    p[0] = p[2]

def p_group_by_clause(parser, p):
    '''group_by_clause : GROUP BY column_list
                       | GROUP BY column_list HAVING condition'''
    if len(p) == 4:
        p[0] = p[3]
    else:
        p[0] = {
            'columns': p[3],
            'having': p[5]
        }

def p_order_by_clause(parser, p):
    'order_by_clause : ORDER BY order_list'
    p[0] = p[3]

def p_limit_clause(parser, p):
    '''limit_clause : LIMIT NUMBER
                    | LIMIT NUMBER OFFSET NUMBER'''
    if len(p) == 3:
        p[0] = p[2]
    else:
        p[0] = {
            'limit': p[2],
            'offset': p[4]
        }

def p_join_clause(parser, p):
    '''join_clause : JOIN ID ON join_condition
                   | JOIN ID ID ON join_condition
                   | JOIN ID AS ID ON join_condition'''
    if len(p) == 5:
        # Simple JOIN table ON condition
        p[0] = {
            'table': p[2],
            'condition': p[4]
        }
    elif len(p) == 6:
        # JOIN table alias ON condition (implicit AS)
        p[0] = {
            'table': p[2],
            'alias': p[3],
            'condition': p[5]
        }
    else:
        # JOIN table AS alias ON condition (explicit AS)
        p[0] = {
            'table': p[2],
            'alias': p[4],
            'condition': p[6]
        }

def p_join_condition(parser, p):
    'join_condition : ID DOT ID EQUALS ID DOT ID'
    p[0] = {
        'left_table': p[1],
        'left_column': p[3],
        'right_table': p[5],
        'right_column': p[7],
        'is_alias_join': True  # Flag this as a join using aliases
    }

def p_projection(parser, p):
    '''projection : TIMES
                  | column_list'''
    if p[1] == '*':
        p[0] = {'type': 'all'}
    else:
        p[0] = {'type': 'columns', 'columns': p[1]}

def p_column_list(parser, p):
    '''column_list : column_item
                   | column_item COMMA column_list'''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = [p[1]] + p[3]

def p_column_item(parser, p):
    '''column_item : ID
                   | ID DOT ID
                   | ID AS ID
                   | ID DOT ID AS ID
                   | aggregate_function
                   | aggregate_function AS ID'''
    if len(p) == 2:
        # Simple column: name
        p[0] = {'type': 'column', 'name': p[1]}
    elif len(p) == 4:
        if p[2] == '.':
            # Qualified column: table.column
            p[0] = {'type': 'column', 'name': f"{p[1]}.{p[3]}"}
        elif p[2].upper() == 'AS':
            # Aliased column: column AS alias
            p[0] = {'type': 'column', 'name': p[1], 'alias': p[3]}
        else:
            # Aggregate function
            p[0] = p[1]
    elif len(p) == 6:
        # Qualified and aliased column: table.column AS alias
        p[0] = {'type': 'column', 'name': f"{p[1]}.{p[3]}", 'alias': p[5]}
    else:
        # Aggregate function with alias
        agg = p[1]
        agg['alias'] = p[3]
        p[0] = agg

def p_aggregate_function(parser, p):
    '''aggregate_function : COUNT LPAREN TIMES RPAREN
                          | COUNT LPAREN ID RPAREN
                          | AVG LPAREN ID RPAREN
                          | SUM LPAREN ID RPAREN
                          | MAX LPAREN ID RPAREN
                          | MIN LPAREN ID RPAREN'''
    func_name = p[1].upper()
    arg = p[3]
    
    if arg == '*' and func_name == 'COUNT':
        arg = '*'
    
    p[0] = {
        'type': 'aggregation',
        'function': func_name,
        'argument': arg
    }

def p_condition(parser, p):
    '''condition : ID EQUALS value
                 | ID GT value
                 | ID LT value
                 | ID GE value
                 | ID LE value
                 | ID NE value
                 | ID DOT ID EQUALS value
                 | ID DOT ID GT value
                 | ID DOT ID LT value
                 | ID DOT ID GE value
                 | ID DOT ID LE value
                 | ID DOT ID NE value
                 | ID IN LPAREN subquery RPAREN
                 | ID DOT ID IN LPAREN subquery RPAREN
                 | condition AND condition
                 | condition OR condition
                 | LPAREN condition RPAREN'''
    if len(p) == 2:
        # Already processed condition (from subexpression)
        p[0] = p[1]
    elif p[1] == '(' and p[3] == ')':
        # Parenthesized condition
        p[0] = p[2]
    elif p[2] in ('AND', 'OR'):
        # Logical operators
        p[0] = {
            'type': p[2].lower(),
            'left': p[1],
            'right': p[3]
        }
    elif p[2] == 'IN' and p[3] == '(':
        # IN subquery
        p[0] = {
            'type': 'in_subquery',
            'column': {'type': 'column', 'name': p[1]},
            'subquery': p[4]
        }
    elif len(p) == 6 and p[4] == '(':
        # Qualified column IN subquery
        p[0] = {
            'type': 'in_subquery',
            'column': {'type': 'column', 'name': f"{p[1]}.{p[3]}"},
            'subquery': p[5]
        }
    elif len(p) == 4:
        # Simple comparison
        p[0] = {
            'type': 'comparison',
            'left': {'type': 'column', 'name': p[1]},
            'operator': p[2],
            'right': p[3]
        }
    else:
        # Qualified column comparison
        p[0] = {
            'type': 'comparison',
            'left': {'type': 'column', 'name': f"{p[1]}.{p[3]}"},
            'operator': p[4],
            'right': p[5]
        }

def p_subquery(parser, p):
    '''subquery : SELECT projection FROM table_reference
                | SELECT projection FROM table_reference where_clause'''
    result = {
        'type': 'SELECT',
        'projection': p[2],
        'table': p[4]
    }
    
    if len(p) > 5:
        result['where'] = p[5]
    
    p[0] = result

def p_where_clause_opt(parser, p):
    '''where_clause_opt : where_clause
                        | '''
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = None

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
    '''insert_statement : INSERT INTO ID VALUES LPAREN value_list RPAREN
                       | INSERT INTO ID LPAREN column_list RPAREN VALUES LPAREN value_list RPAREN'''
    if len(p) == 8:
        # Simple INSERT without column specification
        p[0] = {
            'type': 'INSERT',
            'table_name': p[3],
            'values': p[6]
        }
    else:
        # INSERT with column specification
        p[0] = {
            'type': 'INSERT',
            'table_name': p[3],
            'columns': p[5],
            'values': p[9]
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
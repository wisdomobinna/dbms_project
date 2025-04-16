"""
SQL Lexer module using PLY.
"""
import ply.lex as lex

class SQLLexer(object):
    # List of token names
    tokens = (
        'ID', 'STRING', 'NUMBER',
        'COMMA', 'SEMICOLON', 'DOT',
        'LPAREN', 'RPAREN',
        'EQUALS', 'GT', 'LT', 'GE', 'LE', 'NE',
        # Keywords
        'SELECT', 'FROM', 'WHERE', 'CREATE', 'TABLE', 'INSERT',
        'INTO', 'VALUES', 'DELETE', 'UPDATE', 'SET', 'DROP',
        'INDEX', 'ON', 'PRIMARY', 'KEY', 'INTEGER', 'STRING_TYPE',
        'AND', 'OR', 'ORDER', 'BY', 'HAVING', 'SHOW', 'DESCRIBE',
        'ASC', 'DESC', 'TIMES'  # Added missing tokens
    )
    
    # Regular expression rules for simple tokens
    t_COMMA = r','
    t_SEMICOLON = r';'
    t_DOT = r'\.'
    t_LPAREN = r'\('
    t_RPAREN = r'\)'
    t_EQUALS = r'='
    t_GT = r'>'
    t_LT = r'<'
    t_GE = r'>='
    t_LE = r'<='
    t_NE = r'!='
    t_TIMES = r'\*'  # For SELECT *
    
    # Regular expression rules with actions
    def t_STRING(self, t):
        r"'[^']*'"
        t.value = t.value[1:-1]  # Remove quotes
        return t
    
    def t_NUMBER(self, t):
        r'\d+'
        t.value = int(t.value)
        return t
    
    def t_ID(self, t):
        r'[a-zA-Z_][a-zA-Z0-9_]*'
        # Check for keywords
        t.type = self.reserved.get(t.value.upper(), 'ID')
        return t
    
    # Define a rule to track line numbers
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)
    
    # A string containing ignored characters (spaces and tabs)
    t_ignore = ' \t'
    
    # Error handling rule
    def t_error(self, t):
        print(f"Illegal character '{t.value[0]}'")
        t.lexer.skip(1)
    
    # Build the lexer
    def build(self, **kwargs):
        # Map of reserved words
        self.reserved = {
            'SELECT': 'SELECT', 'FROM': 'FROM', 'WHERE': 'WHERE',
            'CREATE': 'CREATE', 'TABLE': 'TABLE', 'INSERT': 'INSERT',
            'INTO': 'INTO', 'VALUES': 'VALUES', 'DELETE': 'DELETE',
            'UPDATE': 'UPDATE', 'SET': 'SET', 'DROP': 'DROP',
            'INDEX': 'INDEX', 'ON': 'ON', 'PRIMARY': 'PRIMARY',
            'KEY': 'KEY', 'INTEGER': 'INTEGER', 'STRING': 'STRING_TYPE',
            'AND': 'AND', 'OR': 'OR', 'ORDER': 'ORDER', 'BY': 'BY',
            'HAVING': 'HAVING', 'SHOW': 'SHOW', 'DESCRIBE': 'DESCRIBE',
            'ASC': 'ASC', 'DESC': 'DESC'
        }
        self.lexer = lex.lex(module=self, **kwargs)
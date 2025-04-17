# DBMS Project Tests

This directory contains comprehensive tests for the DBMS implementation.

## Test Files

1. **test_basic.py**: Basic functionality tests of all DBMS operations
   - CREATE/DROP TABLE/INDEX
   - INSERT/UPDATE/DELETE
   - SELECT with basic WHERE clauses
   - JOINs
   - Show tables and describe table structure

2. **test_parser_issues.py**: Tests specifically for parser-related issues
   - Multi-line SQL queries
   - String comparisons in WHERE clauses
   - Column naming issues
   - Support for aliases
   - Success messages
   - AND/OR conditions
   - Data type support

3. **test_execution_issues.py**: Tests for execution-specific issues
   - The `__init__` column issue in updates
   - Multiple INSERT formats
   - String WHERE condition execution
   - Complex AND/OR execution
   - Type checking
   - Success messages
   - NULL value handling

4. **test_complex_queries.py**: Tests for advanced SQL features
   - Complex nested conditions
   - Multiple JOINs
   - Multi-statement batches
   - Column and table aliases
   - Subqueries
   - Aggregate functions
   - GROUP BY and HAVING
   - LIMIT and OFFSET clauses

## Running Tests

To run all tests:

```bash
pytest -v
```

To run a specific test file:

```bash
pytest -v tests/test_parser_issues.py
```

To run a specific test:

```bash
pytest -v tests/test_parser_issues.py::TestParserIssues::test_string_where_conditions
```

## Test Issues

These tests focus on the issues mentioned in the requirements:

1. **Multiple line SQL**: Tests ensure multi-line queries work properly
2. **String comparisons in WHERE**: Tests verify string values in WHERE clauses work
3. **`__init__` column issue**: Tests check for unexpected column creation
4. **Aliases support**: Tests attempt aliases and identify limitations
5. **Success messages**: Tests verify meaningful messages are returned
6. **AND/OR support**: Tests complex condition combinations
7. **Type support**: Tests INTEGER and STRING type handling

## Expected Results

- Some tests in `test_complex_queries.py` are expected to fail as they test features that may not yet be implemented.
- These are marked with `pytest.xfail()` to document the needed features.
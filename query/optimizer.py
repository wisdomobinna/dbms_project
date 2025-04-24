"""
Query Optimizer Module

This module handles query optimization, including:
- Join method selection (sort-merge vs. nested-loop)
- Condition ordering optimization
- Query tree transformation
"""
import json
class QueryOptimizer:
    """
    Query Optimizer class that optimizes query execution plans.
    """
    
    def __init__(self, schema_manager, index_manager):
        """
        Initialize the Query Optimizer.
        
        Args:
            schema_manager: The schema manager for metadata
            index_manager: The index manager for index information
        """
        self.schema_manager = schema_manager
        self.index_manager = index_manager
    
    def optimize(self, parsed_query):
        """
        Optimize a parsed query.
        
        Args:
            parsed_query (dict): The parsed query
            
        Returns:
            dict: The optimized query
        """
        # Make a copy of the query to avoid modifying the original
        optimized_query = parsed_query.copy()
        
        # Only optimize SELECT queries
        if parsed_query["type"] != "SELECT":
            return optimized_query
        
        # Optimize WHERE conditions
        if "where" in optimized_query and optimized_query["where"]:
            optimized_query["where"] = self._optimize_conditions(optimized_query["where"], optimized_query["table"])
        
        # Optimize JOIN method selection
        if "join" in optimized_query and optimized_query["join"]:
            # flatten into a list (even if it’s length 1)
            flattened = self._flatten_joins(optimized_query["join"])
            # if you know you'll only ever have one join, just pull it out:
            join = flattened[0] if isinstance(flattened, list) else flattened

            # unwrap left and right table names (in case they're dicts)
            raw_left  = optimized_query["table"]
            raw_right = join["table"]
            left_name  = raw_left["name"]  if isinstance(raw_left, dict) else raw_left
            right_name = raw_right["name"] if isinstance(raw_right, dict) else raw_right

            cond = join["condition"]

            # pick strategy
            strategy = self._select_join_method(left_name, right_name, cond)

            # apply method & columns
            join["method"]       = strategy["method"]
            join["outer_column"] = strategy["outer_column"]
            join["inner_column"] = strategy["inner_column"]

            if strategy.get("swapped", False):
                # swap so executor scans the smaller/outer first
                optimized_query["table"] = strategy["outer"]
                join["table"]           = strategy["inner"]

                # fix the join condition
                cond["left_table"]   = strategy["outer"]
                cond["right_table"]  = strategy["inner"]
                cond["left_column"]  = strategy["outer_column"]
                cond["right_column"] = strategy["inner_column"]

                join["swapped"] = True
            else:
                # no swap
                optimized_query["table"] = strategy["outer"]
                join["table"]            = strategy["inner"]
                join["swapped"]          = False

            # stick it back in
            optimized_query["join"] = join

        print("Final optimized join:")
        print(json.dumps(optimized_query["join"], indent=2))
        optimized_query["execution_plan"] = self._generate_execution_plan(optimized_query)
        return optimized_query
            
    def _optimize_conditions(self, condition, table_name):
        """
        Optimize WHERE conditions by reordering for efficiency.
        
        Args:
            condition (dict): The condition to optimize
            table_name (str): The name of the table
            
        Returns:
            dict: The optimized condition
        """
        # If it's a simple condition, no optimization needed
        if condition["type"] == "comparison":
            # Add selectivity estimate
            condition["selectivity"] = self._estimate_selectivity(table_name, condition)
            return condition
        
        # Handle AND conditions
        if condition["type"] == "and":
            left = self._optimize_conditions(condition["left"], table_name)
            right = self._optimize_conditions(condition["right"], table_name)
            
            # Reorder based on selectivity (most selective first)
            left_selectivity = self._get_condition_selectivity(left)
            right_selectivity = self._get_condition_selectivity(right)
            
            if right_selectivity < left_selectivity:
                # Swap left and right for better performance
                condition["left"] = right
                condition["right"] = left
            else:
                condition["left"] = left
                condition["right"] = right
            
            # Calculate combined selectivity (assuming independence)
            condition["selectivity"] = left_selectivity * right_selectivity
            
            return condition
        
        # Handle OR conditions
        if condition["type"] == "or":
            left = self._optimize_conditions(condition["left"], table_name)
            right = self._optimize_conditions(condition["right"], table_name)
            
            # For OR conditions, we can't easily reorder
            condition["left"] = left
            condition["right"] = right
            
            # Calculate combined selectivity (assuming independence)
            left_selectivity = self._get_condition_selectivity(left)
            right_selectivity = self._get_condition_selectivity(right)
            condition["selectivity"] = left_selectivity + right_selectivity - (left_selectivity * right_selectivity)
            
            return condition
        
        return condition
    
    def _select_join_method(self, from_table, join_table, join_condition):

        
        # Resolve real table names if `from_table` or `join_table` is a dict
        left_table = from_table["name"] if isinstance(from_table, dict) else from_table
        right_table = join_table["table"] if isinstance(join_table, dict) else join_table

        print(f"[JOIN OPTIMIZER] left_table = {left_table}, right_table = {right_table}")

        left_key = join_condition["left_column"]
        right_key = join_condition["right_column"]

        left_size = self.schema_manager.get_record_count(left_table)
        right_size = self.schema_manager.get_record_count(right_table)

        left_indexed = self.schema_manager.index_exists(left_table, left_key)
        right_indexed = self.schema_manager.index_exists(right_table, right_key)

        # Extract aliases if available
        left_alias = from_table.get("alias", left_table) if isinstance(from_table, dict) else left_table
        right_alias = join_table.get("alias", right_table) if isinstance(join_table, dict) else right_table


        # Use hash-join for big sizes
        print(f"left: {left_size}, right: {right_size}")
        if left_size * right_size > 1e7:
            if left_size <= right_size:
                outer, inner        = left_table,  join_table
                outer_col, inner_col= left_key,    right_key
                swapped             = False
            else:
                outer, inner        = join_table, left_table
                outer_col, inner_col= right_key,   left_key
                swapped             = True
            print(f"outer: {outer}, inner: {inner}")

            # TODO Proper alias handling
            return {
                "method":       "hash-join",
                "outer":        outer,
                "inner":        inner,
                "outer_column": outer_col,
                "inner_column": inner_col,
                "outer_alias":  left_alias if not swapped else right_alias,
                "inner_alias":  right_alias if not swapped else left_alias,
                "swapped":      swapped
            }
        # Use index-nested-loop with smaller table as outer
        elif right_indexed and left_size <= right_size:
            print("_select_join: using right_indexed plan with smaller outer")
            print(f"left: {left_size}, right: {right_size}")
            return {
                "method": "index-nested-loop",
                "outer": left_table,
                "inner": right_table,
                "outer_column": left_key,
                "inner_column": right_key,
                "swapped": False
            }
        elif left_indexed and right_size < left_size:
            print("_select_join: using left_indexed plan with smaller outer")
            return {
                "method": "index-nested-loop",
                "outer": right_table,
                "inner": left_table,
                "outer_column": right_key,
                "inner_column": left_key,
                "swapped": True
            }
        # Fallback: use hash-join for very large unindexed tables
        if (not left_indexed and not right_indexed
            and left_size * right_size > 1e7):  # tune threshold as you like
            if left_size <= right_size:
                outer, inner        = left_table,  join_table
                outer_col, inner_col= left_key,    right_key
                swapped             = False
            else:
                outer, inner        = join_table, left_table
                outer_col, inner_col= right_key,   left_key
                swapped             = True

            return {
                "method":       "hash-join",
                "outer":        outer,
                "inner":        inner,
                "outer_column": outer_col,
                "inner_column": inner_col,
                "swapped":      swapped
            }


        # # Fallback: nested-loop with left as outer
        # print("_select_join: fallback to nested-loop join")
        # return {
        #     "method": "nested-loop",
        #     "outer": left_table,
        #     "inner": right_table,
        #     "outer_column": left_key,
        #     "inner_column": right_key,
        #     "swapped": False
        # }


    
    def _estimate_selectivity(self, table_name, condition):
        """
        Estimate the selectivity of a condition (what fraction of records will match).
        
        Args:
            table_name (str): Table name
            condition (dict): The condition
            
        Returns:
            float: Estimated selectivity (0.0 to 1.0)
        """
        if condition["type"] != "comparison":
            return 0.5  # Default for complex conditions
        
        column_name = condition["left"]["name"]
        operator = condition["operator"]
        
        # Check if we have an index on this column
        if self.schema_manager.index_exists(table_name, column_name):
            try:
                # Use index statistics
                total_keys = self.index_manager.get_key_count(table_name, column_name)
                total_records = self.schema_manager.get_record_count(table_name)
                
                # Estimate based on operator
                if operator == "=":
                    # Equality: assume uniform distribution
                    return 1.0 / max(1, total_keys)
                elif operator in ("<", ">", "<=", ">="):
                    # Range: assume roughly half
                    return 0.5
                elif operator in ("!=", "<>"):
                    # Not equal: almost all records
                    return 1.0 - (1.0 / max(1, total_keys))
            except:
                pass
        
        # Default selectivity estimates
        if operator == "=":
            return 0.1  # Equality is usually selective
        elif operator in ("<", ">", "<=", ">="):
            return 0.3  # Range queries select more
        elif operator in ("!=", "<>"):
            return 0.9  # Not equal selects most records
        
        return 0.5  # Default
    
    def _get_condition_selectivity(self, condition):
        """Get the selectivity of a condition, defaulting to 0.5 if not available."""
        return condition.get("selectivity", 0.5)
    
    def _generate_execution_plan(self, query):
        """
        Generate an execution plan for the query.
        
        Args:
            query (dict): The optimized query
            
        Returns:
            dict: Execution plan
        """
        plan = {
            "type": "select",
            "cost": 0.0
        }
        
        # Table access
        table_name = query["table"]
        
        # Handle derived tables (subqueries in FROM)
        if isinstance(table_name, dict) and table_name.get("type") == "derived_table":
            # For derived tables, we use a fixed string identifier
            plan["table_access"] = {
                "table": "derived_table",
                "records": 100,  # Default estimate for subquery
                "method": "subquery"
            }
            record_count = 100  # Default estimate
        else:
            # For regular tables
            if isinstance(table_name, dict) and "name" in table_name:
                # Handle table with alias
                record_table_name = table_name["name"]
            else:
                # Simple table name (ensure it's a string for hashtables)
                record_table_name = str(table_name) if table_name is not None else "unknown"
                
            try:
                # Ensure record_table_name is hashable (a string)
                if isinstance(record_table_name, dict):
                    # Fallback for unexpected dictionary
                    record_count = 100
                else:
                    record_count = self.schema_manager.get_record_count(record_table_name)
            except Exception as e:
                # If table doesn't exist in schema (like for derived tables)
                record_count = 100  # Default estimate
                
            plan["table_access"] = {
                "table": record_table_name if not isinstance(table_name, dict) else table_name["name"],
                "records": record_count,
                "method": "full-scan"
            }
            
        plan["cost"] += record_count  # Each record costs 1 unit
        
        # Filter operation
        if "where" in query and query["where"]:
            selectivity = self._get_condition_selectivity(query["where"])
            plan["filter"] = {
                "condition": self._condition_to_string(query["where"]),
                "selectivity": selectivity,
                "output_records": int(record_count * selectivity)
            }
            plan["cost"] += record_count * 0.1  # Filtering cost
        
        # Join operation
        if "join" in query and query["join"]:
            joins_cost = 0
            join_cost = 0
            if isinstance(query["join"], list):
                # Multiple joins
                joins = []
                current_records = record_count

                for i, join_info in enumerate(query["join"]):
                    join_table = join_info["table"]
                    join_method = join_info.get("method", "nested-loop")
                    join_records = self.schema_manager.get_record_count(join_table)
            
                    if isinstance(join_method, dict):
                        join_strategy = join_method
                        join_method = join_strategy.get("method", "nested-loop")
                    else:
                        join_strategy = {}

                    if join_method == "nested-loop":
                        # Nested loop cost is outer * inner
                        print("Nested looping.")
                        join_cost = current_records * join_records
                    elif join_method == "sort-merge":
                        print("Sort merging.")
                        # Sort-merge cost is cost of sorting both tables plus merging
                        join_cost = current_records * (1 + 0.1 * (1 + min(1, 1000 * current_records))) + \
                                  join_records * (1 + 0.1 * (1 + min(1, 1000 * join_records)))
                    elif join_method == "index-nested-loop":
                        print("Index nested looping.")
                        # Index nested loop uses index lookup for inner table
                        join_cost = current_records * 10  # Assume index lookups are 10x faster
                    else:
                        print(join_method)
                        print("???")
                    condition = join_info["condition"]
                    condition_str = f"{condition.get('left_table', '')}.{condition.get('left_column', '')} = {condition.get('right_table', '')}.{condition.get('right_column', '')}"
                    
                    join_plan = {
                        "table": join_table,
                        "records": join_records,
                        "method": join_method,
                        "condition": condition_str,
                        "cost": join_cost
                    }
                    joins.append(join_plan)
                    joins_cost += join_cost
                    
                    # Update for next join
                    current_records = int(current_records * join_records * 0.1)  # Estimate join output
                
                plan["joins"] = joins
            else:
                # Single join
                join_table = query["join"]["table"]
                join_method = query["join"].get("method", "nested-loop")
                join_records = self.schema_manager.get_record_count(join_table)
                current_records = plan.get("filter", {}).get("output_records", record_count)

                if join_method == "nested-loop":
                    # Nested loop cost is outer * inner
                    join_cost = plan.get("filter", {}).get("output_records", record_count) * join_records
                elif join_method == "sort-merge":
                    # Sort-merge cost is cost of sorting both tables plus merging
                    join_cost = record_count * (1 + 0.1 * (1 + min(1, 1000 * record_count))) + \
                              join_records * (1 + 0.1 * (1 + min(1, 1000 * join_records)))
                elif join_method == "index-nested-loop":
                    print("index-nested-loop")
                    # Index nested loop uses index lookup for inner table
                    join_cost = plan.get("filter", {}).get("output_records", record_count) * 10  # Assume index lookups are 10x faster
                elif join_method == "hash-join":
                    print("Initiating hash-join")
                    outer_count = plan.get("filter", {}).get("output_records", record_count)
                    join_cost   = outer_count + join_records
                else:
                    join_cost = current_records * join_records
                    print("Worst case joining")
                condition = query["join"]["condition"]
                condition_str = f"{condition.get('left_table', '')}.{condition.get('left_column', '')} = {condition.get('right_table', '')}.{condition.get('right_column', '')}"
                
                plan["join"] = {
                    "table": join_table,
                    "records": join_records,
                    "method": join_method,
                    "condition": condition_str,
                    "cost": join_cost
                }
                joins_cost = join_cost
            
            plan["cost"] += joins_cost
        
        # Projection
        # Estimate output records based on join type
        if isinstance(query.get("join"), list) and len(query["join"]) > 0:
            output_records = current_records  # Use the last join's output estimate
        else:
            output_records = plan.get("join", {}).get("output_records", plan.get("filter", {}).get("output_records", record_count))
        
        if query["projection"]["type"] == "all":
            plan["projection"] = {
                "type": "all_columns",
                "cost": output_records
            }
        else:
            columns = [col["name"] if col["type"] == "column" else f"{col['function']}({col['argument']})" 
                      for col in query["projection"]["columns"]]
            plan["projection"] = {
                "type": "columns",
                "columns": columns,
                "cost": output_records * 0.1
            }
        
        plan["cost"] += plan["projection"]["cost"]
        
        # Sorting (ORDER BY)
        if "order_by" in query and query["order_by"]:
            sort_columns = [item["column"] for item in query["order_by"]]
            # Use previously calculated output_records
            sort_cost = output_records * (1 + 0.1 * (1 + min(1, 1000 * output_records)))
            
            plan["sort"] = {
                "columns": sort_columns,
                "cost": sort_cost
            }
            plan["cost"] += sort_cost
        
        return plan
    
    def _condition_to_string(self, condition):
        """Convert a condition to a string representation for the execution plan."""
        if condition["type"] == "comparison":
            left = condition["left"]["name"]
            operator = condition["operator"]
            right = condition["right"]["value"] if condition["right"]["type"] != "column" else condition["right"]["name"]
            return f"{left} {operator} {right}"
        elif condition["type"] == "and":
            return f"({self._condition_to_string(condition['left'])}) AND ({self._condition_to_string(condition['right'])})"
        elif condition["type"] == "or":
            return f"({self._condition_to_string(condition['left'])}) OR ({self._condition_to_string(condition['right'])})"
        return str(condition)
    
    def _flatten_joins(self, joins):
        """
        Flatten a potentially nested join structure.
        
        Args:
            joins: The join structure from the parsed query
            
        Returns:
            list: A flattened list of joins
        """
        if not joins:
            return joins
            
        if not isinstance(joins, list):
            return [joins]
            
        result = []
        
        for item in joins:
            if isinstance(item, dict):
                # Simple join dict
                result.append(item)
            elif isinstance(item, list):
                # Nested list of joins
                result.extend(self._flatten_joins(item))
                
        return result
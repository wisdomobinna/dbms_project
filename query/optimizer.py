"""
Query Optimizer Module

This module handles query optimization, including:
- Join method selection (sort-merge vs. nested-loop)
- Condition ordering optimization
- Query tree transformation
"""

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
            if isinstance(optimized_query["join"], list):
                # Handle multiple joins
                for i, join in enumerate(optimized_query["join"]):
                    optimized_query["join"][i]["method"] = self._select_join_method(
                        optimized_query["table"] if i == 0 else optimized_query["join"][i-1]["table"],
                        join["table"],
                        join["condition"]
                    )
            else:
                # Handle single join
                optimized_query["join"]["method"] = self._select_join_method(
                    optimized_query["table"],
                    optimized_query["join"]["table"],
                    optimized_query["join"]["condition"]
                )
        
        # Add execution plan info
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
    
    def _select_join_method(self, left_table, right_table, join_condition):
        """
        Select the optimal join method based on table statistics.
        
        Args:
            left_table (str): Left table name
            right_table (str): Right table name
            join_condition (dict): Join condition
            
        Returns:
            str: "nested-loop" or "sort-merge"
        """
        left_key = join_condition["left_column"]
        right_key = join_condition["right_column"]
        
        # Check if join columns are indexed
        left_indexed = self.schema_manager.index_exists(left_table, left_key)
        right_indexed = self.schema_manager.index_exists(right_table, right_key)
        
        # Check if join columns are sorted (e.g., primary key)
        left_is_pk = self.schema_manager.get_primary_key(left_table) == left_key
        right_is_pk = self.schema_manager.get_primary_key(right_table) == right_key
        
        # Get table sizes
        left_size = self.schema_manager.get_record_count(left_table)
        right_size = self.schema_manager.get_record_count(right_table)
        
        # If one table is very small, nested loop may be better
        size_ratio = max(left_size, right_size) / max(1, min(left_size, right_size))
        
        # Decision logic
        if left_indexed and right_indexed:
            # Both tables have indexes on join columns
            return "index-nested-loop"
        elif left_is_pk and right_is_pk:
            # Both join columns are primary keys (sorted)
            return "sort-merge"
        elif (left_is_pk or right_is_pk) and size_ratio > 10:
            # One side is a PK and tables have very different sizes
            return "nested-loop"
        elif left_size + right_size < 1000:
            # Small tables, use simple nested loop
            return "nested-loop"
        else:
            # Default to sort-merge for larger tables
            return "sort-merge"
    
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
        record_count = self.schema_manager.get_record_count(table_name)
        plan["table_access"] = {
            "table": table_name,
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
            if isinstance(query["join"], list):
                # Multiple joins
                joins = []
                current_records = record_count
                
                for i, join_info in enumerate(query["join"]):
                    join_table = join_info["table"]
                    join_method = join_info.get("method", "nested-loop")
                    join_records = self.schema_manager.get_record_count(join_table)
                    
                    if join_method == "nested-loop":
                        # Nested loop cost is outer * inner
                        join_cost = current_records * join_records
                    elif join_method == "sort-merge":
                        # Sort-merge cost is cost of sorting both tables plus merging
                        join_cost = current_records * (1 + 0.1 * (1 + min(1, 1000 * current_records))) + \
                                  join_records * (1 + 0.1 * (1 + min(1, 1000 * join_records)))
                    elif join_method == "index-nested-loop":
                        # Index nested loop uses index lookup for inner table
                        join_cost = current_records * 10  # Assume index lookups are 10x faster
                    
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
                
                if join_method == "nested-loop":
                    # Nested loop cost is outer * inner
                    join_cost = plan.get("filter", {}).get("output_records", record_count) * join_records
                elif join_method == "sort-merge":
                    # Sort-merge cost is cost of sorting both tables plus merging
                    join_cost = record_count * (1 + 0.1 * (1 + min(1, 1000 * record_count))) + \
                              join_records * (1 + 0.1 * (1 + min(1, 1000 * join_records)))
                elif join_method == "index-nested-loop":
                    # Index nested loop uses index lookup for inner table
                    join_cost = plan.get("filter", {}).get("output_records", record_count) * 10  # Assume index lookups are 10x faster
                
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
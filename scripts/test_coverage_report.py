#!/usr/bin/env python3
"""
dbt Test Coverage Report Generator

Analyzes dbt models and unit tests to generate coverage reports across:
1. Columns (model columns tested in unit test expectations)
2. Aggregations (SUM, COUNT, AVG, etc.)
3. Operations (math, string, date functions, etc.)
4. Joins (INNER, LEFT, RIGHT, FULL)
"""

import os
import re
import yaml
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple


class TestCoverageAnalyzer:
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.models_dir = self.project_root / "models"
        
        # SQL patterns for different test dimensions
        self.aggregation_patterns = [
            r'\b(SUM|COUNT|AVG|MAX|MIN|STDDEV|VARIANCE)\s*\(',
            r'\b(ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD|FIRST_VALUE|LAST_VALUE)\s*\(',
        ]
        
        self.operation_patterns = [
            r'\b(?!SELECT|FROM|WHERE|JOIN|ON|AND|OR)\w+\s*[\+\-\*\/]\s*(?!FROM|WHERE|JOIN|ON|AND|OR)\w+',  # Math operations (exclude SQL keywords)
            r'\b(ROUND|FLOOR|CEIL|ABS|SQRT|POWER|MOD)\s*\(',  # Math functions
            r'\b(UPPER|LOWER|SUBSTR|SUBSTRING|CONCAT|LENGTH|TRIM|REPLACE)\s*\(',  # String functions
            r'\b(TO_DATE|DATE_TRUNC|EXTRACT|DATEDIFF|DATE_ADD|DATE_SUB)\s*\(',  # Date functions
            r'\b(CASE\s+WHEN|COALESCE|NULLIF|ISNULL|IFNULL)\s*\(',  # Conditional logic
            r'\b(GREATEST|LEAST)\s*\(',  # Comparison functions
            r'\b(ARRAY|ARRAY_AGG|UNNEST)\s*\(',  # Array functions
            r'\b(JSONB?_[A-Z_]+)\s*\(',  # JSON functions (JSON_EXTRACT, JSONB_AGG, etc.)
            r'::[A-Z_]+',  # Type casting
        ]
        
        self.join_patterns = [
            r'\b(INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|JOIN)\b',
        ]

    def find_sql_files(self) -> Dict[str, Path]:
        """Find all SQL model files."""
        sql_files = {}
        for sql_file in self.models_dir.rglob("*.sql"):
            # Extract model name from filename (not full path)
            model_name = sql_file.stem  # Gets filename without .sql extension
            sql_files[model_name] = sql_file
        return sql_files

    def extract_model_columns(self, file_path: Path) -> Set[str]:
        """Extract column names from the final SELECT statement."""
        try:
            content = file_path.read_text()
            
            # Remove comments and strings
            content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)
            content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
            content = re.sub(r"'[^']*'", '', content)
            content = re.sub(r'"[^"]*"', '', content)
            
            select_columns = set()
            model_name = file_path.stem
            
            # Pattern 1: staging models with "renamed as (select ... from source)"
            renamed_pattern = r'renamed\s+as\s*\(\s*select\s+(.*?)\s+from\s+source\s*\)'
            renamed_match = re.search(renamed_pattern, content, re.IGNORECASE | re.DOTALL)
            
            if renamed_match:
                select_clause = renamed_match.group(1).strip()
                columns = [col.strip() for col in select_clause.split(',')]
                
                for col in columns:
                    col = col.strip()
                    if ' as ' in col.lower():
                        alias = col.lower().split(' as ')[-1].strip()
                        select_columns.add(alias)
                    else:
                        clean_col = col.strip().lower()
                        if clean_col and not any(keyword in clean_col for keyword in ['(', ')', 'from', 'where']):
                            select_columns.add(clean_col)
            
            # Pattern 2: Find the very last SELECT statement (after all CTEs)
            else:
                # Split content by 'select' and take the last one
                select_parts = content.split('select')
                if len(select_parts) > 1:
                    last_select = select_parts[-1]
                    
                    # Extract everything between 'select' and 'from'
                    from_match = re.search(r'^(.*?)\s+from\s+', last_select, re.IGNORECASE | re.DOTALL)
                    
                    if from_match:
                        select_clause = from_match.group(1).strip()
                        
                        # Debug output
                        if model_name == 'int_weight_measurements_with_latest_height':
                            print(f"DEBUG - Found select clause: '{select_clause}'")
                        
                        # Split by comma and extract column names
                        columns = [col.strip() for col in select_clause.split(',')]
                        for col in columns:
                            col = col.strip()
                            if ' as ' in col.lower():
                                alias = col.lower().split(' as ')[-1].strip()
                                select_columns.add(alias)
                            else:
                                # Handle table.column references like "weight.user_id"
                                if '.' in col:
                                    clean_col = col.split('.')[-1].strip().lower()
                                else:
                                    clean_col = col.strip().lower()
                                
                                if clean_col and not any(keyword in clean_col for keyword in ['from', 'where', 'join', 'on']):
                                    select_columns.add(clean_col)
            
            # Debug output
            if model_name in ['body_mass_indexes', 'int_weight_measurements_with_latest_height']:
                print(f"DEBUG - {model_name}: {select_columns}")
            
            return select_columns
            
        except Exception as e:
            print(f"Error extracting columns from {file_path}: {e}")
            return set()

    def analyze_sql_file(self, file_path: Path) -> Dict[str, int]:
        """Analyze a SQL file for aggregations, operations, and joins."""
        try:
            content = file_path.read_text().upper()
            
            # Remove comments and strings to avoid false positives
            content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)
            content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
            content = re.sub(r"'[^']*'", '', content)
            content = re.sub(r'"[^"]*"', '', content)
            
            counts = {
                'aggregations': 0,
                'operations': 0,
                'joins': 0
            }
            
            model_name = file_path.stem
            
            # Count aggregations
            for pattern in self.aggregation_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                counts['aggregations'] += len(matches)
            
            # Count operations - look for lines/expressions that contain operations
            operation_lines = set()
            
            # Split content into lines and check each line for operations
            lines = content.split('\n')
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                    
                # Check if this line contains any operation
                has_operation = False
                for pattern in self.operation_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        has_operation = True
                        break
                
                if has_operation:
                    # Use line number + cleaned line content as unique identifier
                    clean_line = re.sub(r'\s+', ' ', line.strip())
                    operation_lines.add(f"{i}:{clean_line}")
            
            counts['operations'] = len(operation_lines)
            
            # Debug output
            if model_name == 'body_mass_indexes':
                print(f"DEBUG - {model_name} operation lines: {operation_lines}")
            
            # Count joins
            for pattern in self.join_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                counts['joins'] += len(matches)
            
            return counts
            
        except Exception as e:
            print(f"Error analyzing {file_path}: {e}")
            return {'aggregations': 0, 'operations': 0, 'joins': 0}

    def find_unit_test_files(self) -> List[Path]:
        """Find all YAML files with unit tests."""
        test_files = []
        for yaml_file in self.models_dir.rglob("*test.yml"):
            test_files.append(yaml_file)
        for yaml_file in self.models_dir.rglob("*_test.yaml"):
            test_files.append(yaml_file)
        return test_files

    def analyze_unit_tests(self, sql_files: Dict[str, Path]) -> Dict[str, Dict[str, any]]:
        """Analyze unit test files to determine coverage."""
        coverage = defaultdict(lambda: {
            'columns': set(),
            'aggregations': 0, 
            'operations': 0, 
            'joins': 0
        })
        
        for test_file in self.find_unit_test_files():
            try:
                with open(test_file, 'r') as f:
                    data = yaml.safe_load(f)
                
                if not data or 'unit_tests' not in data:
                    continue
                
                for test in data['unit_tests']:
                    model_name = test.get('model', '')
                    given_inputs = test.get('given', [])
                    expected_outputs = test.get('expect', {}).get('rows', [])
                    
                    # Column coverage: extract columns from expected results
                    for expected_row in expected_outputs:
                        if isinstance(expected_row, dict):
                            coverage[model_name]['columns'].update(expected_row.keys())
                    
                    # Check for aggregation testing (multiple input rows)
                    has_multiple_rows = any(
                        len(inp.get('rows', [])) > 1 
                        for inp in given_inputs
                    )
                    if has_multiple_rows:
                        coverage[model_name]['aggregations'] = 1
                    
                    # Check for operation testing (at least one input value)
                    has_input_values = any(
                        len(inp.get('rows', [])) > 0 
                        for inp in given_inputs
                    )
                    if has_input_values:
                        coverage[model_name]['operations'] = 1
                    
                    # Check for join testing (multiple input tables)
                    has_multiple_inputs = len(given_inputs) > 1
                    if has_multiple_inputs:
                        coverage[model_name]['joins'] = 1
                        
            except Exception as e:
                print(f"Error analyzing test file {test_file}: {e}")
        
        return dict(coverage)

    def generate_report(self) -> None:
        """Generate and print the coverage report."""
        # Find all models and analyze them
        sql_files = self.find_sql_files()
        model_totals = {}
        model_columns = {}
        
        for model_key, file_path in sql_files.items():
            model_totals[model_key] = self.analyze_sql_file(file_path)
            model_columns[model_key] = self.extract_model_columns(file_path)
        
        # Analyze unit test coverage
        test_coverage = self.analyze_unit_tests(sql_files)
        
        # Print report header
        print("\nTest Coverage Report")
        print("=" * 105)
        print(f"{'Model':<40} {'Columns':<15} {'Aggregations':<15} {'Operations':<15} {'Joins':<15}")
        print("-" * 105)
        
        # Track totals
        total_col_tested = total_col_total = 0
        total_agg_tested = total_agg_total = 0
        total_op_tested = total_op_total = 0
        total_join_tested = total_join_total = 0
        
        # Print model coverage
        for model_key in sorted(model_totals.keys()):
            totals = model_totals[model_key]
            columns = model_columns[model_key]
            coverage = test_coverage.get(model_key, {
                'columns': set(),
                'aggregations': 0, 
                'operations': 0, 
                'joins': 0
            })
            
            # Truncate long model names
            display_name = model_key[:37] + "..." if len(model_key) > 40 else model_key
            
            # Column coverage
            tested_columns = len(coverage['columns'])
            total_columns = len(columns)
            col_coverage = f"{tested_columns}/{total_columns}"
            
            agg_coverage = f"{coverage['aggregations']}/{totals['aggregations']}"
            op_coverage = f"{coverage['operations']}/{totals['operations']}"
            join_coverage = f"{coverage['joins']}/{totals['joins']}"
            
            print(f"{display_name:<40} {col_coverage:<15} {agg_coverage:<15} {op_coverage:<15} {join_coverage:<15}")
            
            # Update totals
            total_col_tested += tested_columns
            total_col_total += total_columns
            total_agg_tested += coverage['aggregations']
            total_agg_total += totals['aggregations']
            total_op_tested += coverage['operations']
            total_op_total += totals['operations']
            total_join_tested += coverage['joins']
            total_join_total += totals['joins']
        
        # Print summary
        print("-" * 105)
        total_col_coverage = f"{total_col_tested}/{total_col_total}"
        total_agg_coverage = f"{total_agg_tested}/{total_agg_total}"
        total_op_coverage = f"{total_op_tested}/{total_op_total}"
        total_join_coverage = f"{total_join_tested}/{total_join_total}"
        
        print(f"{'TOTAL':<40} {total_col_coverage:<15} {total_agg_coverage:<15} {total_op_coverage:<15} {total_join_coverage:<15}")
        
        # Calculate percentages
        col_pct = (total_col_tested / total_col_total * 100) if total_col_total > 0 else 0
        agg_pct = (total_agg_tested / total_agg_total * 100) if total_agg_total > 0 else 0
        op_pct = (total_op_tested / total_op_total * 100) if total_op_total > 0 else 0
        join_pct = (total_join_tested / total_join_total * 100) if total_join_total > 0 else 0
        
        print(f"{'COVERAGE %':<40} {col_pct:<14.1f} {agg_pct:<14.1f} {op_pct:<14.1f} {join_pct:<14.1f}")
        print("=" * 105)


def main():
    """Main entry point."""
    analyzer = TestCoverageAnalyzer()
    analyzer.generate_report()


if __name__ == "__main__":
    main()

"""
Data validation and type checking utilities.

This module provides comprehensive data validation capabilities including
type checking, constraint validation, data quality checks, and performance
optimizations for large datasets.
"""

import re
import pandas as pd
import pyarrow as pa
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
import numpy as np
from dataclasses import dataclass

from src.utils.exceptions import ValidationError
from src.utils.logging import get_logger
from src.config.schemas import get_table_schema


logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of data validation operation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    row_count: int
    column_count: int
    null_counts: Dict[str, int]
    type_issues: List[Dict[str, Any]]
    constraint_violations: List[Dict[str, Any]]
    
    def add_error(self, error: str):
        """Add validation error"""
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        """Add validation warning"""
        self.warnings.append(warning)
    
    def summary(self) -> str:
        """Get validation summary"""
        status = "PASSED" if self.is_valid else "FAILED"
        return (
            f"Validation {status}: {self.row_count} rows, {self.column_count} columns, "
            f"{len(self.errors)} errors, {len(self.warnings)} warnings"
        )


class DataValidator:
    """
    Comprehensive data validator for backup system.
    
    Provides validation for data types, constraints, data quality,
    and performance optimized validation for large datasets.
    """
    
    def __init__(self):
        self.validation_rules = {}
        self.custom_validators = {}
        
        # Initialize default validation rules
        self._setup_default_rules()
    
    def _setup_default_rules(self):
        """Setup default validation rules"""
        self.validation_rules = {
            'required_fields': [],
            'unique_fields': [],
            'email_fields': [],
            'phone_fields': [],
            'currency_fields': [],
            'date_fields': [],
            'positive_numeric_fields': [],
            'percentage_fields': [],
            'max_length': {},
            'min_length': {},
            'regex_patterns': {},
            'range_constraints': {},
            'enum_constraints': {}
        }
    
    def validate_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: Optional[str] = None,
        rules: Optional[Dict[str, Any]] = None,
        sample_size: Optional[int] = None
    ) -> ValidationResult:
        """
        Validate pandas DataFrame against schema and business rules.
        
        Args:
            df: DataFrame to validate
            table_name: Optional table name for schema lookup
            rules: Optional custom validation rules
            sample_size: Optional sample size for large datasets
        
        Returns:
            ValidationResult with validation details
        """
        logger.debug(
            f"Starting DataFrame validation",
            table_name=table_name,
            rows=len(df),
            columns=len(df.columns)
        )
        
        # Initialize result
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            row_count=len(df),
            column_count=len(df.columns),
            null_counts={},
            type_issues=[],
            constraint_violations=[]
        )
        
        try:
            # Sample data for large datasets
            if sample_size and len(df) > sample_size:
                df_sample = df.sample(n=sample_size, random_state=42)
                result.add_warning(f"Validating sample of {sample_size} rows from {len(df)} total rows")
            else:
                df_sample = df
            
            # Schema validation if table name provided
            if table_name:
                self._validate_against_schema(df_sample, table_name, result)
            
            # Basic data quality checks
            self._validate_data_quality(df_sample, result)
            
            # Business rule validation
            validation_rules = rules or self.validation_rules
            self._validate_business_rules(df_sample, validation_rules, result)
            
            # Performance and statistics
            self._collect_statistics(df_sample, result)
            
            logger.info(result.summary())
            return result
            
        except Exception as e:
            logger.error(f"Validation failed with error: {e}")
            result.add_error(f"Validation process failed: {e}")
            return result
    
    def validate_pyarrow_table(
        self, 
        table: pa.Table, 
        table_name: Optional[str] = None,
        rules: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """
        Validate PyArrow table.
        
        Args:
            table: PyArrow table to validate
            table_name: Optional table name for schema lookup
            rules: Optional custom validation rules
        
        Returns:
            ValidationResult with validation details
        """
        # Convert to DataFrame for validation (could be optimized for large tables)
        df = table.to_pandas()
        return self.validate_dataframe(df, table_name, rules)
    
    def _validate_against_schema(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        result: ValidationResult
    ):
        """Validate DataFrame against defined table schema"""
        schema = get_table_schema(table_name)
        if not schema:
            result.add_warning(f"No schema found for table {table_name}")
            return
        
        schema_fields = {field.name: field for field in schema}
        df_columns = set(df.columns)
        
        # Check for missing required columns
        required_columns = {name for name, field in schema_fields.items() if not field.nullable}
        missing_required = required_columns - df_columns
        if missing_required:
            result.add_error(f"Missing required columns: {list(missing_required)}")
        
        # Check for extra columns not in schema
        extra_columns = df_columns - set(schema_fields.keys())
        if extra_columns:
            result.add_warning(f"Extra columns not in schema: {list(extra_columns)}")
        
        # Validate column types
        for column_name in df_columns.intersection(schema_fields.keys()):
            field = schema_fields[column_name]
            self._validate_column_type(df, column_name, field, result)
    
    def _validate_column_type(
        self, 
        df: pd.DataFrame, 
        column_name: str, 
        field: pa.Field, 
        result: ValidationResult
    ):
        """Validate specific column against schema field"""
        column = df[column_name]
        field_type = field.type
        
        # Skip validation for all-null columns if nullable
        if column.isna().all() and field.nullable:
            return
        
        # Type-specific validation
        try:
            if pa.types.is_integer(field_type):
                self._validate_integer_column(column, column_name, result)
            elif pa.types.is_floating(field_type):
                self._validate_float_column(column, column_name, result)
            elif pa.types.is_string(field_type):
                self._validate_string_column(column, column_name, result)
            elif pa.types.is_timestamp(field_type):
                self._validate_timestamp_column(column, column_name, result)
            elif pa.types.is_date(field_type):
                self._validate_date_column(column, column_name, result)
            elif pa.types.is_decimal(field_type):
                self._validate_decimal_column(column, column_name, field_type, result)
            elif pa.types.is_boolean(field_type):
                self._validate_boolean_column(column, column_name, result)
                
        except Exception as e:
            result.type_issues.append({
                'column': column_name,
                'expected_type': str(field_type),
                'error': str(e)
            })
    
    def _validate_integer_column(self, column: pd.Series, column_name: str, result: ValidationResult):
        """Validate integer column"""
        non_null = column.dropna()
        if len(non_null) == 0:
            return
        
        # Check if values can be converted to integers
        try:
            # Try converting to numeric first
            numeric_values = pd.to_numeric(non_null, errors='coerce')
            invalid_count = numeric_values.isna().sum()
            
            if invalid_count > 0:
                result.type_issues.append({
                    'column': column_name,
                    'issue': f'{invalid_count} values cannot be converted to integer',
                    'type': 'integer_conversion'
                })
            
            # Check for decimal values that would be truncated
            valid_numeric = numeric_values.dropna()
            if len(valid_numeric) > 0:
                decimal_count = (valid_numeric != valid_numeric.astype(int)).sum()
                if decimal_count > 0:
                    result.add_warning(f"Column {column_name}: {decimal_count} decimal values will be truncated")
                    
        except Exception as e:
            result.type_issues.append({
                'column': column_name,
                'issue': f'Integer validation failed: {e}',
                'type': 'integer_validation'
            })
    
    def _validate_float_column(self, column: pd.Series, column_name: str, result: ValidationResult):
        """Validate float column"""
        non_null = column.dropna()
        if len(non_null) == 0:
            return
        
        try:
            numeric_values = pd.to_numeric(non_null, errors='coerce')
            invalid_count = numeric_values.isna().sum()
            
            if invalid_count > 0:
                result.type_issues.append({
                    'column': column_name,
                    'issue': f'{invalid_count} values cannot be converted to float',
                    'type': 'float_conversion'
                })
            
            # Check for infinite values
            valid_numeric = numeric_values.dropna()
            if len(valid_numeric) > 0:
                inf_count = np.isinf(valid_numeric).sum()
                if inf_count > 0:
                    result.add_warning(f"Column {column_name}: {inf_count} infinite values found")
                    
        except Exception as e:
            result.type_issues.append({
                'column': column_name,
                'issue': f'Float validation failed: {e}',
                'type': 'float_validation'
            })
    
    def _validate_string_column(self, column: pd.Series, column_name: str, result: ValidationResult):
        """Validate string column"""
        non_null = column.dropna()
        if len(non_null) == 0:
            return
        
        # Check for very long strings that might cause issues
        if hasattr(non_null, 'str'):
            max_length = non_null.str.len().max()
            if max_length > 10000:  # 10KB limit
                result.add_warning(f"Column {column_name}: Maximum string length is {max_length} characters")
            
            # Check for empty strings
            empty_count = (non_null.str.len() == 0).sum()
            if empty_count > 0:
                result.add_warning(f"Column {column_name}: {empty_count} empty strings found")
    
    def _validate_timestamp_column(self, column: pd.Series, column_name: str, result: ValidationResult):
        """Validate timestamp column"""
        non_null = column.dropna()
        if len(non_null) == 0:
            return
        
        try:
            # Try converting to datetime
            datetime_values = pd.to_datetime(non_null, errors='coerce')
            invalid_count = datetime_values.isna().sum()
            
            if invalid_count > 0:
                result.type_issues.append({
                    'column': column_name,
                    'issue': f'{invalid_count} values cannot be converted to timestamp',
                    'type': 'timestamp_conversion'
                })
            
            # Check for future dates (might be data quality issue)
            valid_dates = datetime_values.dropna()
            if len(valid_dates) > 0:
                now = pd.Timestamp.now()
                future_count = (valid_dates > now).sum()
                if future_count > 0:
                    result.add_warning(f"Column {column_name}: {future_count} future timestamps found")
                    
        except Exception as e:
            result.type_issues.append({
                'column': column_name,
                'issue': f'Timestamp validation failed: {e}',
                'type': 'timestamp_validation'
            })
    
    def _validate_date_column(self, column: pd.Series, column_name: str, result: ValidationResult):
        """Validate date column"""
        # Similar to timestamp validation but for dates
        self._validate_timestamp_column(column, column_name, result)
    
    def _validate_decimal_column(
        self, 
        column: pd.Series, 
        column_name: str, 
        decimal_type: pa.DataType, 
        result: ValidationResult
    ):
        """Validate decimal column"""
        non_null = column.dropna()
        if len(non_null) == 0:
            return
        
        try:
            # Extract precision and scale from decimal type
            precision = decimal_type.precision
            scale = decimal_type.scale
            
            # Convert to numeric and check constraints
            numeric_values = pd.to_numeric(non_null, errors='coerce')
            invalid_count = numeric_values.isna().sum()
            
            if invalid_count > 0:
                result.type_issues.append({
                    'column': column_name,
                    'issue': f'{invalid_count} values cannot be converted to decimal',
                    'type': 'decimal_conversion'
                })
            
            # Check precision and scale constraints
            valid_numeric = numeric_values.dropna()
            if len(valid_numeric) > 0:
                # Check if values fit within precision/scale constraints
                max_value = 10 ** (precision - scale) - 1
                min_value = -(10 ** (precision - scale) - 1)
                
                overflow_count = ((valid_numeric > max_value) | (valid_numeric < min_value)).sum()
                if overflow_count > 0:
                    result.add_warning(
                        f"Column {column_name}: {overflow_count} values exceed decimal({precision},{scale}) range"
                    )
                    
        except Exception as e:
            result.type_issues.append({
                'column': column_name,
                'issue': f'Decimal validation failed: {e}',
                'type': 'decimal_validation'
            })
    
    def _validate_boolean_column(self, column: pd.Series, column_name: str, result: ValidationResult):
        """Validate boolean column"""
        non_null = column.dropna()
        if len(non_null) == 0:
            return
        
        # Check if values can be converted to boolean
        valid_bool_values = {True, False, 1, 0, '1', '0', 'true', 'false', 'True', 'False', 'TRUE', 'FALSE'}
        invalid_values = ~non_null.isin(valid_bool_values)
        
        if invalid_values.any():
            invalid_count = invalid_values.sum()
            result.type_issues.append({
                'column': column_name,
                'issue': f'{invalid_count} values cannot be converted to boolean',
                'type': 'boolean_conversion'
            })
    
    def _validate_data_quality(self, df: pd.DataFrame, result: ValidationResult):
        """Perform general data quality checks"""
        # Calculate null counts
        for column in df.columns:
            null_count = df[column].isna().sum()
            result.null_counts[column] = null_count
            
            # Warn about high null percentages
            null_percentage = (null_count / len(df)) * 100
            if null_percentage > 50:
                result.add_warning(f"Column {column} has {null_percentage:.1f}% null values")
        
        # Check for duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            result.add_warning(f"Found {duplicate_count} duplicate rows")
        
        # Check for completely empty columns
        empty_columns = [col for col in df.columns if df[col].isna().all()]
        if empty_columns:
            result.add_warning(f"Completely empty columns: {empty_columns}")
    
    def _validate_business_rules(
        self, 
        df: pd.DataFrame, 
        rules: Dict[str, Any], 
        result: ValidationResult
    ):
        """Validate business-specific rules"""
        # Required fields check
        for field in rules.get('required_fields', []):
            if field in df.columns:
                null_count = df[field].isna().sum()
                if null_count > 0:
                    result.add_error(f"Required field {field} has {null_count} null values")
        
        # Unique fields check
        for field in rules.get('unique_fields', []):
            if field in df.columns:
                duplicate_count = df[field].duplicated().sum()
                if duplicate_count > 0:
                    result.add_error(f"Unique field {field} has {duplicate_count} duplicate values")
        
        # Email validation
        for field in rules.get('email_fields', []):
            if field in df.columns:
                self._validate_email_field(df[field], field, result)
        
        # Currency validation
        for field in rules.get('currency_fields', []):
            if field in df.columns:
                self._validate_currency_field(df[field], field, result)
        
        # Range constraints
        for field, (min_val, max_val) in rules.get('range_constraints', {}).items():
            if field in df.columns:
                self._validate_range_constraint(df[field], field, min_val, max_val, result)
        
        # Custom validators
        for field, validator_func in self.custom_validators.items():
            if field in df.columns:
                try:
                    validator_func(df[field], result)
                except Exception as e:
                    result.add_error(f"Custom validation failed for {field}: {e}")
    
    def _validate_email_field(self, column: pd.Series, column_name: str, result: ValidationResult):
        """Validate email field format"""
        non_null = column.dropna()
        if len(non_null) == 0:
            return
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        invalid_emails = ~non_null.str.match(email_pattern, na=False)
        
        if invalid_emails.any():
            invalid_count = invalid_emails.sum()
            result.constraint_violations.append({
                'column': column_name,
                'violation': f'{invalid_count} invalid email formats',
                'type': 'email_format'
            })
    
    def _validate_currency_field(self, column: pd.Series, column_name: str, result: ValidationResult):
        """Validate currency field values"""
        non_null = column.dropna()
        if len(non_null) == 0:
            return
        
        valid_currencies = {'USD', 'CAD', 'EUR', 'GBP', 'CNY', 'JPY'}  # Add more as needed
        invalid_currencies = ~non_null.isin(valid_currencies)
        
        if invalid_currencies.any():
            invalid_count = invalid_currencies.sum()
            unique_invalid = non_null[invalid_currencies].unique()
            result.constraint_violations.append({
                'column': column_name,
                'violation': f'{invalid_count} invalid currency codes: {list(unique_invalid)[:5]}',
                'type': 'currency_code'
            })
    
    def _validate_range_constraint(
        self, 
        column: pd.Series, 
        column_name: str, 
        min_val: Any, 
        max_val: Any, 
        result: ValidationResult
    ):
        """Validate range constraints"""
        non_null = column.dropna()
        if len(non_null) == 0:
            return
        
        try:
            numeric_column = pd.to_numeric(non_null, errors='coerce')
            valid_values = numeric_column.dropna()
            
            if len(valid_values) > 0:
                below_min = (valid_values < min_val).sum()
                above_max = (valid_values > max_val).sum()
                
                if below_min > 0:
                    result.constraint_violations.append({
                        'column': column_name,
                        'violation': f'{below_min} values below minimum {min_val}',
                        'type': 'range_constraint'
                    })
                
                if above_max > 0:
                    result.constraint_violations.append({
                        'column': column_name,
                        'violation': f'{above_max} values above maximum {max_val}',
                        'type': 'range_constraint'
                    })
                    
        except Exception as e:
            result.add_warning(f"Range validation failed for {column_name}: {e}")
    
    def _collect_statistics(self, df: pd.DataFrame, result: ValidationResult):
        """Collect basic statistics about the data"""
        result.row_count = len(df)
        result.column_count = len(df.columns)
        
        # Memory usage
        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        if memory_mb > 100:  # Warn about large memory usage
            result.add_warning(f"DataFrame uses {memory_mb:.1f} MB of memory")
    
    def add_custom_validator(self, field_name: str, validator_func: Callable):
        """Add custom validation function for a field"""
        self.custom_validators[field_name] = validator_func
        logger.info(f"Added custom validator for field {field_name}")
    
    def set_validation_rules(self, rules: Dict[str, Any]):
        """Set validation rules"""
        self.validation_rules.update(rules)
        logger.info("Updated validation rules")


# Global validator instance
_data_validator = DataValidator()

def get_data_validator() -> DataValidator:
    """Get the global data validator instance"""
    return _data_validator

def validate_data(
    data: Union[pd.DataFrame, pa.Table], 
    table_name: Optional[str] = None,
    rules: Optional[Dict[str, Any]] = None
) -> ValidationResult:
    """Convenience function to validate data"""
    validator = get_data_validator()
    
    if isinstance(data, pd.DataFrame):
        return validator.validate_dataframe(data, table_name, rules)
    elif isinstance(data, pa.Table):
        return validator.validate_pyarrow_table(data, table_name, rules)
    else:
        raise ValidationError(f"Unsupported data type for validation: {type(data)}")


def quick_validate(data: Union[pd.DataFrame, pa.Table]) -> bool:
    """Quick validation that returns only pass/fail"""
    result = validate_data(data)
    return result.is_valid
"""
⚠️  DEPRECATED: Static Schema Management System

🚨 WARNING: This module contains hardcoded table schemas that pose a HIGH RISK
of schema drift and Parquet compatibility issues with Redshift Spectrum.

❌ PROBLEMS:
- Hardcoded schemas become stale when database structure evolves
- Creates inconsistencies between backup stages using different schema sources  
- Caused production Parquet schema compatibility errors
- No automatic detection of schema changes

✅ SOLUTION:
Use FlexibleSchemaManager for dynamic schema discovery from actual database:

    from src.core.flexible_schema_manager import FlexibleSchemaManager
    from src.core.connections import ConnectionManager
    from src.config.settings import AppConfig
    config = AppConfig()
    connection_manager = ConnectionManager(config)
    schema_manager = FlexibleSchemaManager(connection_manager)
    schema_info = schema_manager.get_table_schema(table_name)

📋 MIGRATION STATUS:
- Validation system: ✅ MIGRATED to FlexibleSchemaManager
- Backup strategies: ✅ Already using FlexibleSchemaManager
- Loading system: ✅ COMPLETED - migrated to FlexibleSchemaManager
- Test files: ⚠️ TODO - update remaining test script imports (non-critical)

🗑️  SCHEDULED FOR REMOVAL: This module will be removed in v1.2.0
"""

import pyarrow as pa
from typing import Dict, Optional, List, Any, Union
from datetime import datetime
import json
from pathlib import Path

from src.utils.exceptions import ValidationError, ConfigurationError
from src.utils.logging import get_logger


logger = get_logger(__name__)


# Common field types and patterns
COMMON_FIELDS = {
    'id': pa.field('id', pa.int64(), nullable=False),
    'created_at': pa.field('created_at', pa.timestamp('us'), nullable=True),
    'updated_at': pa.field('updated_at', pa.timestamp('us'), nullable=True),
    'update_at': pa.field('update_at', pa.timestamp('us'), nullable=True),  # Legacy naming
    'deleted_at': pa.field('deleted_at', pa.timestamp('us'), nullable=True),
    'is_active': pa.field('is_active', pa.bool_(), nullable=True),
    'status': pa.field('status', pa.string(), nullable=True),
    'version': pa.field('version', pa.int32(), nullable=True),
}

# Settlement table schemas based on the original project requirements
TABLE_SCHEMAS = {
    # Main settlement table
    'settlement.settlement_normal_delivery_detail': pa.schema([
        pa.field('ID', pa.int64(), nullable=False),
        pa.field('billing_num', pa.string(), nullable=True),
        pa.field('ref_billing_num', pa.string(), nullable=True),
        pa.field('order_num', pa.string(), nullable=True),
        pa.field('parcel_num', pa.string(), nullable=True),
        pa.field('customer_id', pa.int32(), nullable=True),
        pa.field('customer_name', pa.string(), nullable=True),
        pa.field('partner_id', pa.int32(), nullable=True),
        pa.field('partner_name', pa.string(), nullable=True),
        pa.field('service_type', pa.string(), nullable=True),
        pa.field('service_level', pa.string(), nullable=True),
        pa.field('origin_country', pa.string(), nullable=True),
        pa.field('dest_country', pa.string(), nullable=True),
        pa.field('dest_province', pa.string(), nullable=True),
        pa.field('dest_city', pa.string(), nullable=True),
        pa.field('dest_postal_code', pa.string(), nullable=True),
        pa.field('weight_kg', pa.float64(), nullable=True),
        pa.field('length_cm', pa.float64(), nullable=True),
        pa.field('width_cm', pa.float64(), nullable=True),
        pa.field('height_cm', pa.float64(), nullable=True),
        pa.field('volume_weight_kg', pa.float64(), nullable=True),
        pa.field('chargeable_weight_kg', pa.float64(), nullable=True),
        pa.field('base_fee', pa.decimal128(15, 4), nullable=True),
        pa.field('fuel_surcharge', pa.decimal128(15, 4), nullable=True),
        pa.field('remote_surcharge', pa.decimal128(15, 4), nullable=True),
        pa.field('oversized_surcharge', pa.decimal128(15, 4), nullable=True),
        pa.field('total_fee', pa.decimal128(15, 4), nullable=True),
        pa.field('currency', pa.string(), nullable=True),
        pa.field('exchange_rate', pa.decimal128(15, 6), nullable=True),
        pa.field('settlement_date', pa.date32(), nullable=True),
        pa.field('billing_period', pa.string(), nullable=True),
        pa.field('tracking_status', pa.string(), nullable=True),
        pa.field('delivery_status', pa.string(), nullable=True),
        pa.field('created_at', pa.timestamp('us'), nullable=True),
        pa.field('updated_at', pa.timestamp('us'), nullable=True),
        pa.field('update_at', pa.timestamp('us'), nullable=True),  # For incremental processing
    ]),
    
    # Claim details table - Updated with actual database schema (18 columns)
    'settlement.settlement_claim_detail': pa.schema([
        pa.field('ID', pa.int32(), nullable=False),
        pa.field('billing_num', pa.string(), nullable=False),
        pa.field('partner_id', pa.int32(), nullable=False),
        pa.field('customer_id', pa.int32(), nullable=True),
        pa.field('ant_parcel_no', pa.string(), nullable=False),
        pa.field('warehouse', pa.string(), nullable=True),
        pa.field('shipping_fee', pa.decimal128(15, 4), nullable=False),
        pa.field('declared_value', pa.decimal128(15, 4), nullable=False),
        pa.field('total', pa.decimal128(15, 4), nullable=False),
        pa.field('dollar_100_limit_adjustment', pa.decimal128(15, 4), nullable=False),
        pa.field('charge_currency', pa.string(), nullable=True),
        pa.field('reason', pa.string(), nullable=True),
        pa.field('is_valid', pa.int32(), nullable=True),
        pa.field('create_at', pa.timestamp('us'), nullable=True),
        pa.field('update_at', pa.timestamp('us'), nullable=True),
        pa.field('orig_declared_value', pa.decimal128(15, 4), nullable=False),
        pa.field('orig_currency', pa.string(), nullable=True),
        pa.field('exchange_rate', pa.decimal128(15, 4), nullable=True),
    ]),
    
    # Transshipment details
    'settlement.transshipment_detail': pa.schema([
        pa.field('ID', pa.int64(), nullable=False),
        pa.field('transshipment_num', pa.string(), nullable=True),
        pa.field('parcel_num', pa.string(), nullable=True),
        pa.field('origin_partner_id', pa.int32(), nullable=True),
        pa.field('dest_partner_id', pa.int32(), nullable=True),
        pa.field('transshipment_type', pa.string(), nullable=True),
        pa.field('weight_kg', pa.float64(), nullable=True),
        pa.field('transshipment_fee', pa.decimal128(10, 4), nullable=True),
        pa.field('currency', pa.string(), nullable=True),
        pa.field('transshipment_date', pa.date32(), nullable=True),
        pa.field('settlement_date', pa.date32(), nullable=True),
        pa.field('status', pa.string(), nullable=True),
        pa.field('created_at', pa.timestamp('us'), nullable=True),
        pa.field('updated_at', pa.timestamp('us'), nullable=True),
        pa.field('update_at', pa.timestamp('us'), nullable=True),
    ]),
    
    # Fee calculation details
    'settlement.fee_calculation_detail': pa.schema([
        pa.field('ID', pa.int64(), nullable=False),
        pa.field('parcel_num', pa.string(), nullable=True),
        pa.field('fee_type', pa.string(), nullable=True),
        pa.field('fee_category', pa.string(), nullable=True),
        pa.field('calculation_method', pa.string(), nullable=True),
        pa.field('base_rate', pa.decimal128(10, 6), nullable=True),
        pa.field('quantity', pa.float64(), nullable=True),
        pa.field('unit', pa.string(), nullable=True),
        pa.field('fee_amount', pa.decimal128(10, 4), nullable=True),
        pa.field('currency', pa.string(), nullable=True),
        pa.field('rate_card_id', pa.int32(), nullable=True),
        pa.field('zone', pa.string(), nullable=True),
        pa.field('effective_date', pa.date32(), nullable=True),
        pa.field('created_at', pa.timestamp('us'), nullable=True),
        pa.field('updated_at', pa.timestamp('us'), nullable=True),
        pa.field('update_at', pa.timestamp('us'), nullable=True),
    ]),
    
    # Partner information
    'settlement.partner_info': pa.schema([
        pa.field('ID', pa.int64(), nullable=False),
        pa.field('partner_id', pa.int32(), nullable=False),
        pa.field('partner_name', pa.string(), nullable=True),
        pa.field('partner_code', pa.string(), nullable=True),
        pa.field('partner_type', pa.string(), nullable=True),
        pa.field('country', pa.string(), nullable=True),
        pa.field('region', pa.string(), nullable=True),
        pa.field('contact_email', pa.string(), nullable=True),
        pa.field('is_active', pa.bool_(), nullable=True),
        pa.field('created_at', pa.timestamp('us'), nullable=True),
        pa.field('updated_at', pa.timestamp('us'), nullable=True),
        pa.field('update_at', pa.timestamp('us'), nullable=True),
    ]),
    
    # Rate card information
    'settlement.rate_card': pa.schema([
        pa.field('ID', pa.int64(), nullable=False),
        pa.field('rate_card_id', pa.int32(), nullable=False),
        pa.field('partner_id', pa.int32(), nullable=True),
        pa.field('service_type', pa.string(), nullable=True),
        pa.field('origin_zone', pa.string(), nullable=True),
        pa.field('dest_zone', pa.string(), nullable=True),
        pa.field('weight_from_kg', pa.float64(), nullable=True),
        pa.field('weight_to_kg', pa.float64(), nullable=True),
        pa.field('base_rate', pa.decimal128(10, 6), nullable=True),
        pa.field('additional_rate', pa.decimal128(10, 6), nullable=True),
        pa.field('currency', pa.string(), nullable=True),
        pa.field('effective_from', pa.date32(), nullable=True),
        pa.field('effective_to', pa.date32(), nullable=True),
        pa.field('is_active', pa.bool_(), nullable=True),
        pa.field('created_at', pa.timestamp('us'), nullable=True),
        pa.field('updated_at', pa.timestamp('us'), nullable=True),
        pa.field('update_at', pa.timestamp('us'), nullable=True),
    ]),
}


class SchemaManager:
    """
    Manages table schemas and provides validation capabilities.
    
    This class handles schema definitions, validation, evolution,
    and provides utilities for working with different table schemas.
    """
    
    def __init__(self, custom_schemas: Optional[Dict[str, pa.Schema]] = None):
        self.schemas = TABLE_SCHEMAS.copy()
        if custom_schemas:
            self.schemas.update(custom_schemas)
        
        logger.info("Static schema manager initialized (deprecated - use flexible_schema_manager for dynamic discovery)")
    
    def get_schema(self, table_name: str) -> Optional[pa.Schema]:
        """
        Get schema for a specific table.
        
        Args:
            table_name: Name of the table
        
        Returns:
            PyArrow schema or None if not found
        """
        schema = self.schemas.get(table_name)
        if schema:
            logger.debug(f"Retrieved schema for table {table_name}")
        else:
            logger.warning(f"No schema found for table {table_name}")
        return schema
    
    def validate_data(
        self, 
        data: Union[pa.Table, Dict[str, Any], List[Dict[str, Any]]], 
        table_name: str,
        strict: bool = True
    ) -> pa.Table:
        """
        Validate data against table schema.
        
        Args:
            data: Data to validate (PyArrow table, dict, or list of dicts)
            table_name: Name of the table schema to validate against
            strict: Whether to enforce strict validation
        
        Returns:
            Validated PyArrow table
        
        Raises:
            ValidationError: If validation fails
        """
        schema = self.get_schema(table_name)
        if not schema:
            if strict:
                raise ValidationError(f"No schema defined for table: {table_name}")
            else:
                logger.warning(f"No schema for {table_name}, proceeding without validation")
                # Convert data to table without schema validation
                if isinstance(data, pa.Table):
                    return data
                elif isinstance(data, (dict, list)):
                    import pandas as pd
                    df = pd.DataFrame(data if isinstance(data, list) else [data])
                    return pa.Table.from_pandas(df)
        
        try:
            # Convert input data to PyArrow table
            if isinstance(data, pa.Table):
                table = data
            elif isinstance(data, (dict, list)):
                import pandas as pd
                df = pd.DataFrame(data if isinstance(data, list) else [data])
                table = pa.Table.from_pandas(df)
            else:
                raise ValidationError(f"Unsupported data type: {type(data)}")
            
            # Validate schema compatibility
            self._validate_schema_compatibility(table.schema, schema, table_name, strict)
            
            # Cast to target schema if possible
            if strict:
                # Strict mode: ensure exact schema match
                validated_table = table.cast(schema)
            else:
                # Flexible mode: handle missing/extra columns
                validated_table = self._flexible_schema_cast(table, schema)
            
            logger.debug(
                f"Data validation successful for {table_name}",
                rows=len(validated_table),
                columns=len(validated_table.column_names)
            )
            
            return validated_table
            
        except pa.ArrowException as e:
            logger.error(f"Schema validation failed for {table_name}: {e}")
            raise ValidationError(f"Schema validation failed for {table_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected validation error for {table_name}: {e}")
            raise ValidationError(f"Validation error for {table_name}: {e}")
    
    def _validate_schema_compatibility(
        self, 
        data_schema: pa.Schema, 
        target_schema: pa.Schema, 
        table_name: str,
        strict: bool
    ):
        """Validate that data schema is compatible with target schema"""
        data_fields = {field.name: field for field in data_schema}
        target_fields = {field.name: field for field in target_schema}
        
        # Check for missing required fields
        missing_fields = []
        for field_name, field in target_fields.items():
            if field_name not in data_fields and not field.nullable:
                missing_fields.append(field_name)
        
        if missing_fields:
            error_msg = f"Missing required fields in {table_name}: {missing_fields}"
            if strict:
                raise ValidationError(error_msg)
            else:
                logger.warning(error_msg)
        
        # Check for type compatibility
        incompatible_fields = []
        for field_name, data_field in data_fields.items():
            if field_name in target_fields:
                target_field = target_fields[field_name]
                if not self._types_compatible(data_field.type, target_field.type):
                    incompatible_fields.append({
                        'field': field_name,
                        'data_type': str(data_field.type),
                        'expected_type': str(target_field.type)
                    })
        
        if incompatible_fields and strict:
            raise ValidationError(f"Type incompatibilities in {table_name}: {incompatible_fields}")
        elif incompatible_fields:
            logger.warning(f"Type incompatibilities in {table_name}: {incompatible_fields}")
    
    def _flexible_schema_cast(self, table: pa.Table, target_schema: pa.Schema) -> pa.Table:
        """Perform flexible schema casting with missing/extra column handling"""
        target_fields = {field.name: field for field in target_schema}
        
        # Prepare columns for the target schema
        new_columns = []
        new_names = []
        
        for field in target_schema:
            field_name = field.name
            new_names.append(field_name)
            
            if field_name in table.column_names:
                # Column exists, try to cast
                try:
                    column = table.column(field_name).cast(field.type)
                    new_columns.append(column)
                except pa.ArrowException:
                    # Cast failed, create null column
                    logger.warning(f"Failed to cast column {field_name}, using nulls")
                    null_array = pa.nulls(len(table), field.type)
                    new_columns.append(null_array)
            else:
                # Column missing, create null column
                logger.debug(f"Column {field_name} missing, creating null column")
                null_array = pa.nulls(len(table), field.type)
                new_columns.append(null_array)
        
        return pa.table(new_columns, names=new_names)
    
    def _types_compatible(self, data_type: pa.DataType, target_type: pa.DataType) -> bool:
        """Check if two PyArrow types are compatible"""
        # Exact match
        if data_type == target_type:
            return True
        
        # String types are generally compatible
        if pa.types.is_string(data_type) and pa.types.is_string(target_type):
            return True
        
        # Numeric type compatibility
        if pa.types.is_integer(data_type) and pa.types.is_integer(target_type):
            return True
        
        if pa.types.is_floating(data_type) and pa.types.is_floating(target_type):
            return True
        
        # Integer to float is usually acceptable
        if pa.types.is_integer(data_type) and pa.types.is_floating(target_type):
            return True
        
        # Timestamp compatibility
        if pa.types.is_timestamp(data_type) and pa.types.is_timestamp(target_type):
            return True
        
        return False
    
    def add_schema(self, table_name: str, schema: pa.Schema):
        """Add or update a table schema"""
        self.schemas[table_name] = schema
        logger.info(f"Added schema for table {table_name}")
    
    def remove_schema(self, table_name: str) -> bool:
        """Remove a table schema"""
        if table_name in self.schemas:
            del self.schemas[table_name]
            logger.info(f"Removed schema for table {table_name}")
            return True
        return False
    
    def list_tables(self) -> List[str]:
        """Get list of all tables with defined schemas"""
        return list(self.schemas.keys())
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a table schema"""
        schema = self.get_schema(table_name)
        if not schema:
            return None
        
        fields_info = []
        for field in schema:
            fields_info.append({
                'name': field.name,
                'type': str(field.type),
                'nullable': field.nullable,
                'metadata': dict(field.metadata) if field.metadata else {}
            })
        
        return {
            'table_name': table_name,
            'field_count': len(schema),
            'fields': fields_info,
            'metadata': dict(schema.metadata) if schema.metadata else {}
        }
    
    def export_schemas(self, output_path: str):
        """Export all schemas to a JSON file"""
        schemas_dict = {}
        for table_name, schema in self.schemas.items():
            schemas_dict[table_name] = {
                'fields': [
                    {
                        'name': field.name,
                        'type': str(field.type),
                        'nullable': field.nullable
                    }
                    for field in schema
                ]
            }
        
        with open(output_path, 'w') as f:
            json.dump(schemas_dict, f, indent=2)
        
        logger.info(f"Exported {len(schemas_dict)} schemas to {output_path}")
    
    def import_schemas(self, input_path: str):
        """Import schemas from a JSON file"""
        with open(input_path, 'r') as f:
            schemas_dict = json.load(f)
        
        for table_name, schema_info in schemas_dict.items():
            fields = []
            for field_info in schema_info['fields']:
                # This is a simplified import - would need more sophisticated type parsing
                field_type = self._parse_type_string(field_info['type'])
                field = pa.field(
                    field_info['name'],
                    field_type,
                    nullable=field_info.get('nullable', True)
                )
                fields.append(field)
            
            self.schemas[table_name] = pa.schema(fields)
        
        logger.info(f"Imported {len(schemas_dict)} schemas from {input_path}")
    
    def _parse_type_string(self, type_str: str) -> pa.DataType:
        """Parse a type string back to PyArrow type (simplified)"""
        type_mapping = {
            'int64': pa.int64(),
            'int32': pa.int32(),
            'float64': pa.float64(),
            'string': pa.string(),
            'bool': pa.bool_(),
            'date32[day]': pa.date32(),
            'timestamp[us]': pa.timestamp('us'),
        }
        
        # Handle decimal types
        if type_str.startswith('decimal128'):
            # Extract precision and scale from string like "decimal128(10, 4)"
            import re
            match = re.search(r'decimal128\((\d+),\s*(\d+)\)', type_str)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return pa.decimal128(precision, scale)
        
        return type_mapping.get(type_str, pa.string())  # Default to string


# Global schema manager instance (deprecated - use flexible_schema_manager instead)
_schema_manager = None

def get_schema_manager() -> SchemaManager:
    """Get the global schema manager instance (deprecated)"""
    global _schema_manager
    if _schema_manager is None:
        _schema_manager = SchemaManager()
    return _schema_manager

def get_table_schema(table_name: str) -> Optional[pa.Schema]:
    """
    ⚠️ DEPRECATED: Get table schema (DANGEROUS - use FlexibleSchemaManager instead)
    
    🚨 CRITICAL WARNING: This function uses hardcoded schemas that become stale!
    
    Use dynamic schema discovery instead:
        from src.core.flexible_schema_manager import FlexibleSchemaManager
        from src.core.connections import ConnectionManager
        from src.config.settings import AppConfig
        config = AppConfig()
        connection_manager = ConnectionManager(config)
        schema_manager = FlexibleSchemaManager(connection_manager)
        schema_info = schema_manager.get_table_schema(table_name)
        schema = schema_info.pyarrow_schema
    """
    import warnings
    warnings.warn(
        f"get_table_schema() is deprecated and dangerous for {table_name}. "
        f"Use FlexibleSchemaManager for dynamic schema discovery.",
        DeprecationWarning,
        stacklevel=2
    )
    logger.warning(f"🚨 Using DEPRECATED hardcoded schema for {table_name} - high risk of schema mismatch!")
    return get_schema_manager().get_schema(table_name)

def validate_table_data(data: Any, table_name: str, strict: bool = True) -> pa.Table:
    """
    ⚠️ DEPRECATED: Validate table data (DANGEROUS - use FlexibleSchemaManager instead)
    
    🚨 CRITICAL WARNING: This function uses hardcoded schemas that cause Parquet errors!
    
    Use dynamic validation instead:
        from src.core.flexible_schema_manager import FlexibleSchemaManager
        from src.core.connections import ConnectionManager
        from src.config.settings import AppConfig
        config = AppConfig()
        connection_manager = ConnectionManager(config)
        schema_manager = FlexibleSchemaManager(connection_manager)
        schema_info = schema_manager.get_table_schema(table_name)
        # Use schema_info.pyarrow_schema for validation
    """
    import warnings
    warnings.warn(
        f"validate_table_data() is deprecated and dangerous for {table_name}. "
        f"Use FlexibleSchemaManager for dynamic schema validation.",
        DeprecationWarning,
        stacklevel=2
    )
    logger.warning(f"🚨 Using DEPRECATED hardcoded validation for {table_name} - high risk of Parquet errors!")
    return get_schema_manager().validate_data(data, table_name, strict)
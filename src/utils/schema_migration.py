"""
Schema Migration and Drift Detection Utilities

This module provides utilities to detect schema drift between hardcoded schemas
and actual database structures, and helps migrate away from dangerous static schemas.
"""

import pyarrow as pa
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from pathlib import Path

from src.utils.logging import get_logger
from src.utils.exceptions import ValidationError

logger = get_logger(__name__)


@dataclass
class SchemaComparison:
    """Result of comparing two schemas"""
    table_name: str
    is_compatible: bool
    field_differences: List[str]
    precision_differences: List[str]
    nullable_differences: List[str]
    missing_fields: List[str]
    extra_fields: List[str]


class SchemaDriftDetector:
    """
    Detects schema drift between hardcoded schemas and actual database structure
    """
    
    def __init__(self):
        self.drift_reports: List[SchemaComparison] = []
        
    def compare_schemas(self, static_schema: pa.Schema, dynamic_schema: pa.Schema, table_name: str) -> SchemaComparison:
        """
        Compare static (hardcoded) schema with dynamic (database) schema
        
        Args:
            static_schema: Schema from hardcoded TABLE_SCHEMAS
            dynamic_schema: Schema from actual database discovery
            table_name: Name of the table being compared
            
        Returns:
            SchemaComparison with detailed differences
        """
        comparison = SchemaComparison(
            table_name=table_name,
            is_compatible=True,
            field_differences=[],
            precision_differences=[],
            nullable_differences=[],
            missing_fields=[],
            extra_fields=[]
        )
        
        # Create field mappings
        static_fields = {field.name: field for field in static_schema}
        dynamic_fields = {field.name: field for field in dynamic_schema}
        
        # Check for missing fields (in static but not in dynamic)
        missing_fields = set(static_fields.keys()) - set(dynamic_fields.keys())
        comparison.missing_fields = list(missing_fields)
        
        # Check for extra fields (in dynamic but not in static)
        extra_fields = set(dynamic_fields.keys()) - set(static_fields.keys())
        comparison.extra_fields = list(extra_fields)
        
        # Check common fields for differences
        common_fields = set(static_fields.keys()) & set(dynamic_fields.keys())
        
        for field_name in common_fields:
            static_field = static_fields[field_name]
            dynamic_field = dynamic_fields[field_name]
            
            # Type differences
            if not self._types_compatible(static_field.type, dynamic_field.type):
                diff = f"{field_name}: static={static_field.type} vs dynamic={dynamic_field.type}"
                comparison.field_differences.append(diff)
                comparison.is_compatible = False
                
                # Special handling for decimal precision differences
                if pa.types.is_decimal(static_field.type) and pa.types.is_decimal(dynamic_field.type):
                    static_prec = f"({static_field.type.precision},{static_field.type.scale})"
                    dynamic_prec = f"({dynamic_field.type.precision},{dynamic_field.type.scale})"
                    precision_diff = f"{field_name}: static decimal{static_prec} vs dynamic decimal{dynamic_prec}"
                    comparison.precision_differences.append(precision_diff)
            
            # Nullable differences
            if static_field.nullable != dynamic_field.nullable:
                nullable_diff = f"{field_name}: static nullable={static_field.nullable} vs dynamic nullable={dynamic_field.nullable}"
                comparison.nullable_differences.append(nullable_diff)
                comparison.is_compatible = False
        
        # Overall compatibility check
        if missing_fields or extra_fields or comparison.field_differences:
            comparison.is_compatible = False
        
        return comparison
    
    def _types_compatible(self, type1: pa.DataType, type2: pa.DataType) -> bool:
        """
        Check if two PyArrow types are compatible
        
        Args:
            type1: First type to compare
            type2: Second type to compare
            
        Returns:
            True if types are compatible
        """
        # Exact type match
        if type1.equals(type2):
            return True
        
        # Compatible integer types
        if pa.types.is_integer(type1) and pa.types.is_integer(type2):
            # Different integer sizes are mostly compatible
            return True
        
        # Compatible string types
        if pa.types.is_string(type1) and pa.types.is_string(type2):
            return True
        
        # Compatible timestamp types
        if pa.types.is_timestamp(type1) and pa.types.is_timestamp(type2):
            # Different timestamp units are mostly compatible
            return True
        
        # Compatible decimal types (but precision matters)
        if pa.types.is_decimal(type1) and pa.types.is_decimal(type2):
            # Decimals with different precision/scale are not compatible for Parquet
            return (type1.precision == type2.precision and type1.scale == type2.scale)
        
        # Compatible float types
        if pa.types.is_floating(type1) and pa.types.is_floating(type2):
            return True
        
        return False
    
    def detect_all_drift(self) -> List[SchemaComparison]:
        """
        Detect schema drift for all hardcoded schemas
        
        Returns:
            List of schema comparisons showing drift
        """
        try:
            # Import here to avoid circular dependencies
            from src.config.schemas import TABLE_SCHEMAS
            from src.core.flexible_schema_manager import FlexibleSchemaManager
            from src.core.connections import ConnectionManager
            from src.config.settings import AppConfig
            
            config = AppConfig()
            connection_manager = ConnectionManager(config)
            schema_manager = FlexibleSchemaManager(connection_manager)
            drift_results = []
            
            logger.info(f"Checking schema drift for {len(TABLE_SCHEMAS)} hardcoded schemas...")
            
            for table_name, static_schema in TABLE_SCHEMAS.items():
                try:
                    # Get dynamic schema from database
                    schema_info = schema_manager.get_table_schema(table_name)
                    if not schema_info or not hasattr(schema_info, 'pyarrow_schema'):
                        logger.warning(f"Could not get dynamic schema for {table_name}")
                        continue
                    
                    dynamic_schema = schema_info.pyarrow_schema
                    
                    # Compare schemas
                    comparison = self.compare_schemas(static_schema, dynamic_schema, table_name)
                    drift_results.append(comparison)
                    
                    # Log results
                    if comparison.is_compatible:
                        logger.info(f"✅ Schema compatible: {table_name}")
                    else:
                        logger.warning(f"❌ Schema drift detected: {table_name}")
                        for diff in comparison.field_differences:
                            logger.warning(f"  Type difference: {diff}")
                        for diff in comparison.precision_differences:
                            logger.warning(f"  Precision difference: {diff}")
                        if comparison.missing_fields:
                            logger.warning(f"  Missing fields: {comparison.missing_fields}")
                        if comparison.extra_fields:
                            logger.warning(f"  Extra fields: {comparison.extra_fields}")
                
                except Exception as e:
                    logger.error(f"Failed to check schema drift for {table_name}: {e}")
            
            self.drift_reports = drift_results
            return drift_results
            
        except ImportError:
            logger.warning("Could not import schema modules for drift detection")
            return []
    
    def generate_drift_report(self, output_path: Optional[str] = None) -> str:
        """
        Generate a comprehensive drift report
        
        Args:
            output_path: Optional path to save the report
            
        Returns:
            Report content as string
        """
        if not self.drift_reports:
            self.detect_all_drift()
        
        report_lines = [
            "# Schema Drift Detection Report",
            f"Generated at: {pa.lib.native_utcnow()}",
            "",
            "## Summary",
            f"Total tables checked: {len(self.drift_reports)}",
            f"Compatible schemas: {sum(1 for r in self.drift_reports if r.is_compatible)}",
            f"Incompatible schemas: {sum(1 for r in self.drift_reports if not r.is_compatible)}",
            ""
        ]
        
        # Detailed results
        report_lines.append("## Detailed Results")
        report_lines.append("")
        
        for comparison in self.drift_reports:
            status = "✅ COMPATIBLE" if comparison.is_compatible else "❌ INCOMPATIBLE"
            report_lines.append(f"### {comparison.table_name} - {status}")
            report_lines.append("")
            
            if comparison.field_differences:
                report_lines.append("**Type Differences:**")
                for diff in comparison.field_differences:
                    report_lines.append(f"- {diff}")
                report_lines.append("")
            
            if comparison.precision_differences:
                report_lines.append("**Precision Differences:**")
                for diff in comparison.precision_differences:
                    report_lines.append(f"- {diff}")
                report_lines.append("")
            
            if comparison.missing_fields:
                report_lines.append(f"**Missing Fields:** {', '.join(comparison.missing_fields)}")
                report_lines.append("")
            
            if comparison.extra_fields:
                report_lines.append(f"**Extra Fields:** {', '.join(comparison.extra_fields)}")
                report_lines.append("")
        
        report_content = "\n".join(report_lines)
        
        if output_path:
            with open(output_path, 'w') as f:
                f.write(report_content)
            logger.info(f"Schema drift report saved to: {output_path}")
        
        return report_content


def check_schema_drift_on_startup():
    """
    Check for schema drift during application startup and log warnings
    """
    try:
        detector = SchemaDriftDetector()
        drift_results = detector.detect_all_drift()
        
        incompatible_count = sum(1 for r in drift_results if not r.is_compatible)
        
        if incompatible_count > 0:
            logger.warning(f"🚨 SCHEMA DRIFT DETECTED: {incompatible_count} tables have schema mismatches")
            logger.warning("This could cause Parquet compatibility issues with Redshift Spectrum")
            logger.warning("Consider migrating to dynamic schema discovery")
        else:
            logger.info(f"✅ Schema drift check passed: {len(drift_results)} tables compatible")
            
    except Exception as e:
        logger.error(f"Schema drift detection failed: {e}")


if __name__ == "__main__":
    # CLI interface for schema drift detection
    import sys
    
    detector = SchemaDriftDetector()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--report":
        output_file = "schema_drift_report.md" if len(sys.argv) < 3 else sys.argv[2]
        report = detector.generate_drift_report(output_file)
        print(f"Schema drift report generated: {output_file}")
    else:
        drift_results = detector.detect_all_drift()
        incompatible = [r for r in drift_results if not r.is_compatible]
        
        if incompatible:
            print(f"❌ Schema drift detected in {len(incompatible)} tables:")
            for result in incompatible:
                print(f"  - {result.table_name}")
            sys.exit(1)
        else:
            print(f"✅ All {len(drift_results)} schemas are compatible")
            sys.exit(0)
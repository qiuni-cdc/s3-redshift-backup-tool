#!/usr/bin/env python3
"""
Fix watermark scoping issue by creating a unified table name resolver.

This ensures watermarks are saved and read with consistent table names,
regardless of whether they include connection scope prefixes.
"""

import re
from typing import Optional, Tuple


class TableNameResolver:
    """
    Resolves table names consistently for watermark operations.
    
    Handles both scoped (CONNECTION:schema.table) and unscoped (schema.table) formats.
    """
    
    @staticmethod
    def parse_table_name(table_name: str) -> Tuple[Optional[str], str]:
        """
        Parse a table name into connection scope and base name.
        
        Args:
            table_name: Table name with or without scope
            
        Returns:
            Tuple of (connection_scope, base_table_name)
            
        Examples:
            "US_PROD_RO_SSH:kuaisong.uni_prealert_order" -> ("US_PROD_RO_SSH", "kuaisong.uni_prealert_order")
            "kuaisong.uni_prealert_order" -> (None, "kuaisong.uni_prealert_order")
        """
        if ':' in table_name:
            parts = table_name.split(':', 1)
            return parts[0], parts[1]
        return None, table_name
    
    @staticmethod
    def get_watermark_key(table_name: str, use_scope: bool = True) -> str:
        """
        Get consistent watermark key for a table.
        
        Args:
            table_name: Table name with or without scope
            use_scope: If True, include scope in key; if False, use base name only
            
        Returns:
            Consistent key for watermark storage
        """
        connection_scope, base_name = TableNameResolver.parse_table_name(table_name)
        
        if use_scope and connection_scope:
            # Use scoped name for v1.2.0 pipelines
            return f"{connection_scope}:{base_name}"
        else:
            # Use base name only for backward compatibility
            return base_name
    
    @staticmethod
    def normalize_for_backup(table_name: str) -> str:
        """
        Normalize table name for backup operations.
        
        Always returns the scoped version if a scope exists.
        """
        connection_scope, base_name = TableNameResolver.parse_table_name(table_name)
        if connection_scope:
            return f"{connection_scope}:{base_name}"
        return base_name


def test_table_name_resolver():
    """Test the table name resolver with various formats."""
    test_cases = [
        ("US_PROD_RO_SSH:kuaisong.uni_prealert_order", True, "US_PROD_RO_SSH:kuaisong.uni_prealert_order"),
        ("US_PROD_RO_SSH:kuaisong.uni_prealert_order", False, "kuaisong.uni_prealert_order"),
        ("kuaisong.uni_prealert_order", True, "kuaisong.uni_prealert_order"),
        ("kuaisong.uni_prealert_order", False, "kuaisong.uni_prealert_order"),
        ("schema.table", True, "schema.table"),
        ("CONNECTION:schema.table", True, "CONNECTION:schema.table"),
    ]
    
    print("Testing TableNameResolver:")
    for table_name, use_scope, expected in test_cases:
        result = TableNameResolver.get_watermark_key(table_name, use_scope)
        status = "✅" if result == expected else "❌"
        print(f"  {status} {table_name} (scope={use_scope}) -> {result}")
    
    # Test parsing
    print("\nTesting parse_table_name:")
    parse_tests = [
        ("US_PROD_RO_SSH:kuaisong.uni_prealert_order", ("US_PROD_RO_SSH", "kuaisong.uni_prealert_order")),
        ("kuaisong.uni_prealert_order", (None, "kuaisong.uni_prealert_order")),
    ]
    
    for table_name, expected in parse_tests:
        result = TableNameResolver.parse_table_name(table_name)
        status = "✅" if result == expected else "❌"
        print(f"  {status} {table_name} -> {result}")


if __name__ == "__main__":
    test_table_name_resolver()
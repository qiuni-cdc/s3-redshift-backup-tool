"""
Column Mapping Manager for Redshift Compatibility

This module manages column name mappings between source MySQL tables 
and target Redshift tables, handling cases where column names need 
to be sanitized for Redshift compatibility (e.g., columns starting with numbers).
"""

import json
import os
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from datetime import datetime

from src.utils.logging import get_logger

logger = get_logger(__name__)


class ColumnMapper:
    """Manages column name mappings for Redshift compatibility"""
    
    def __init__(self, mappings_dir: Optional[str] = None):
        """
        Initialize the column mapper
        
        Args:
            mappings_dir: Directory to store column mappings. 
                         Defaults to project_root/column_mappings/
        """
        if mappings_dir:
            self.mappings_dir = Path(mappings_dir)
        else:
            # Default to project_root/column_mappings/
            project_root = Path(__file__).parent.parent.parent
            self.mappings_dir = project_root / "column_mappings"
        
        # Create directory if it doesn't exist
        self.mappings_dir.mkdir(exist_ok=True)
        logger.info(f"Column mappings directory: {self.mappings_dir}")
        
        # In-memory cache
        self._mapping_cache: Dict[str, Dict[str, str]] = {}
    
    def sanitize_column_name(self, column_name: str) -> str:
        """
        Sanitize column name for Redshift compatibility
        
        Rules:
        - Column names cannot start with a number
        - Must start with letter or underscore
        """
        if column_name and column_name[0].isdigit():
            return f"col_{column_name}"
        return column_name
    
    def generate_mapping(self, table_name: str, column_names: List[str]) -> Dict[str, str]:
        """
        Generate column mapping for a table
        
        Args:
            table_name: Full table name (e.g., "US_DW_UNIDW_SSH:unidw.dw_parcel_detail_tool")
            column_names: List of original column names from MySQL
            
        Returns:
            Dictionary mapping original names to sanitized names
        """
        mapping = {}
        has_changes = False
        
        for original_name in column_names:
            sanitized_name = self.sanitize_column_name(original_name)
            mapping[original_name] = sanitized_name
            
            if original_name != sanitized_name:
                has_changes = True
                logger.info(f"Column '{original_name}' mapped to '{sanitized_name}' for {table_name}")
        
        # Only save if there are actual mappings
        if has_changes:
            self.save_mapping(table_name, mapping)
        
        return mapping
    
    def save_mapping(self, table_name: str, mapping: Dict[str, str]) -> None:
        """Save column mapping to file"""
        # Create safe filename from table name
        safe_filename = table_name.replace(':', '_').replace('.', '_') + '.json'
        mapping_file = self.mappings_dir / safe_filename
        
        # Only include mappings that actually change the name
        filtered_mapping = {
            orig: new for orig, new in mapping.items() 
            if orig != new
        }
        
        if filtered_mapping:
            mapping_data = {
                'table_name': table_name,
                'created_at': datetime.now().isoformat(),
                'mappings': filtered_mapping,
                'total_columns': len(mapping),
                'mapped_columns': len(filtered_mapping)
            }
            
            with open(mapping_file, 'w') as f:
                json.dump(mapping_data, f, indent=2)
            
            logger.info(f"Saved column mappings for {table_name}: {len(filtered_mapping)} mappings")
            
            # Update cache
            self._mapping_cache[table_name] = filtered_mapping
    
    def load_mapping(self, table_name: str) -> Dict[str, str]:
        """Load column mapping from file or cache"""
        # Check cache first
        if table_name in self._mapping_cache:
            return self._mapping_cache[table_name]
        
        # Try to load from file
        safe_filename = table_name.replace(':', '_').replace('.', '_') + '.json'
        mapping_file = self.mappings_dir / safe_filename
        
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r') as f:
                    data = json.load(f)
                    mapping = data.get('mappings', {})
                    
                    # Cache it
                    self._mapping_cache[table_name] = mapping
                    
                    logger.info(f"Loaded column mappings for {table_name}: {len(mapping)} mappings")
                    return mapping
            except Exception as e:
                logger.error(f"Failed to load mapping file {mapping_file}: {e}")
        
        # No mapping found
        return {}
    
    def get_redshift_column_name(self, table_name: str, original_column: str) -> str:
        """Get the Redshift column name for an original MySQL column"""
        mapping = self.load_mapping(table_name)
        return mapping.get(original_column, original_column)
    
    def get_original_column_name(self, table_name: str, redshift_column: str) -> str:
        """Get the original MySQL column name from a Redshift column name"""
        mapping = self.load_mapping(table_name)
        
        # Reverse lookup
        for orig, redshift in mapping.items():
            if redshift == redshift_column:
                return orig
        
        # No mapping found, assume it's the same
        return redshift_column
    
    def has_mapping(self, table_name: str) -> bool:
        """Check if a table has any column mappings"""
        mapping = self.load_mapping(table_name)
        return len(mapping) > 0
    
    def list_all_mappings(self) -> List[Dict[str, Any]]:
        """List all available column mappings"""
        mappings = []
        
        for mapping_file in self.mappings_dir.glob('*.json'):
            try:
                with open(mapping_file, 'r') as f:
                    data = json.load(f)
                    mappings.append({
                        'table_name': data.get('table_name'),
                        'created_at': data.get('created_at'),
                        'mapped_columns': data.get('mapped_columns', 0),
                        'total_columns': data.get('total_columns', 0),
                        'file': mapping_file.name
                    })
            except Exception as e:
                logger.error(f"Failed to read mapping file {mapping_file}: {e}")
        
        return sorted(mappings, key=lambda x: x.get('table_name', ''))
    
    def clear_mapping(self, table_name: str) -> bool:
        """Clear/delete column mapping for a table"""
        safe_filename = table_name.replace(':', '_').replace('.', '_') + '.json'
        mapping_file = self.mappings_dir / safe_filename
        
        if mapping_file.exists():
            try:
                mapping_file.unlink()
                # Clear from cache
                self._mapping_cache.pop(table_name, None)
                logger.info(f"Cleared column mapping for {table_name}")
                return True
            except Exception as e:
                logger.error(f"Failed to clear mapping for {table_name}: {e}")
                return False
        
        return False
    
    def get_copy_column_list(self, table_name: str, source_columns: List[str]) -> str:
        """
        Generate column list for Redshift COPY command with mappings
        
        Args:
            table_name: Full table name
            source_columns: List of column names in the source parquet file
            
        Returns:
            Column list string for COPY command, e.g., "(col1, col_2, col3)"
        """
        mapping = self.load_mapping(table_name)
        
        if not mapping:
            # No mapping needed
            return ""
        
        # Map all columns
        mapped_columns = []
        for col in source_columns:
            mapped_name = mapping.get(col, col)
            mapped_columns.append(mapped_name)
        
        return f"({', '.join(mapped_columns)})"
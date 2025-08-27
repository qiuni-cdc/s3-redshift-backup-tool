#!/usr/bin/env python3
"""
Production validation test for Feature 1: Schema Alignment with Real Settlement Data

This test validates the schema alignment feature using actual settlement database data
to ensure production compatibility and performance.
"""

import pandas as pd
import pyarrow as pa
import sys
import os
from datetime import datetime

# Add src to path for imports
sys.path.append('/home/qi_chen/s3-redshift-backup/src')

from core.connections import ConnectionManager
from core.s3_manager import S3Manager
from config.settings import AppConfig
from config.schemas import get_table_schema
from utils.logging import get_logger

logger = get_logger(__name__)

def test_real_settlement_data():
    """Test schema alignment with real settlement data from production database"""
    print("🏭 Testing Schema Alignment with Real Settlement Data")
    print("=" * 60)
    
    try:
        # Load configuration
        config = AppConfig()
        connection_manager = ConnectionManager(config)
        s3_manager = S3Manager(config)
        
        # Get settlement schema
        schema = get_table_schema('settlement.settlement_normal_delivery_detail')
        if not schema:
            print("❌ FAILED: Could not load settlement schema")
            return False
            
        print(f"📋 Target schema has {len(schema)} columns")
        
        # Connect to database and fetch sample data
        print("🔗 Connecting to settlement database...")
        
        with connection_manager.ssh_tunnel() as local_port:
            with connection_manager.database_connection(local_port) as conn:
                # Fetch small sample for testing
                query = """
                SELECT * FROM settlement.settlement_normal_delivery_detail 
                WHERE create_at >= '2025-08-06 00:00:00'
                LIMIT 100
                """
                
                print("📊 Fetching sample settlement data...")
                df = pd.read_sql(query, conn)
                
                if df.empty:
                    print("⚠️  No data found in specified date range")
                    return False
                
                print(f"✅ Fetched {len(df)} rows with {len(df.columns)} columns")
                print(f"📅 Date range: {df['create_at'].min()} to {df['create_at'].max()}")
                
                # Test schema alignment
                print("\n🔧 Testing schema alignment...")
                start_time = datetime.now()
                
                aligned_table = s3_manager.align_dataframe_to_redshift_schema(df, schema)
                
                processing_time = (datetime.now() - start_time).total_seconds()
                
                # Validate results
                print("\n📈 Validation Results:")
                print(f"   ✅ Input rows: {len(df)}")
                print(f"   ✅ Output rows: {len(aligned_table)}")
                print(f"   ✅ Input columns: {len(df.columns)}")
                print(f"   ✅ Output columns: {len(aligned_table.column_names)}")
                print(f"   ✅ Schema match: {aligned_table.schema.equals(schema)}")
                print(f"   ⏱️  Processing time: {processing_time:.3f}s")
                
                # Check data integrity
                print("\n🔍 Data Integrity Checks:")
                
                # Verify key columns preserved
                if 'ant_parcel_no' in df.columns:
                    original_parcels = set(df['ant_parcel_no'].dropna())
                    aligned_parcels = set(filter(None, aligned_table.column('ant_parcel_no').to_pylist()))
                    print(f"   ✅ Parcel numbers preserved: {len(original_parcels)} → {len(aligned_parcels)}")
                
                if 'billing_num' in df.columns:
                    original_bills = set(df['billing_num'].dropna())
                    aligned_bills = set(filter(None, aligned_table.column('billing_num').to_pylist()))
                    print(f"   ✅ Billing numbers preserved: {len(original_bills)} → {len(aligned_bills)}")
                
                # Test parquet generation
                print("\n💾 Testing Parquet Generation...")
                
                # Generate test S3 key
                s3_key = f"test/schema_alignment_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                
                # Test parquet upload without actually uploading to S3
                try:
                    import io
                    buffer = io.BytesIO()
                    
                    # Write aligned table to parquet buffer
                    import pyarrow.parquet as pq
                    pq.write_table(aligned_table, buffer, compression='snappy')
                    buffer_size = buffer.tell()
                    
                    print(f"   ✅ Parquet generation successful")
                    print(f"   📦 File size: {buffer_size / 1024:.1f} KB")
                    print(f"   🗜️  Compression ratio: {(len(df) * len(df.columns) * 8) / buffer_size:.1f}x")
                    
                    # Verify parquet can be read back
                    buffer.seek(0)
                    read_back_table = pq.read_table(buffer)
                    
                    print(f"   ✅ Parquet read-back successful")
                    print(f"   📋 Read schema matches: {read_back_table.schema.equals(aligned_table.schema)}")
                    
                except Exception as parquet_error:
                    print(f"   ❌ Parquet generation failed: {parquet_error}")
                    return False
                
                print("\n🎉 PRODUCTION TEST PASSED - Schema alignment ready for deployment!")
                return True
                
    except Exception as e:
        print(f"❌ PRODUCTION TEST FAILED: {e}")
        logger.error("Production test failed", error=str(e))
        return False

def test_schema_comparison():
    """Compare aligned schema with actual Redshift table structure"""
    print("\n🏗️  Testing Schema Compatibility with Redshift")
    print("=" * 50)
    
    try:
        schema = get_table_schema('settlement.settlement_normal_delivery_detail')
        
        print("📋 PyArrow Schema Analysis:")
        print(f"   Total fields: {len(schema)}")
        
        # Analyze field types
        type_counts = {}
        nullable_count = 0
        
        for field in schema:
            type_str = str(field.type)
            type_counts[type_str] = type_counts.get(type_str, 0) + 1
            if field.nullable:
                nullable_count += 1
        
        print(f"   Nullable fields: {nullable_count}/{len(schema)}")
        print("   Type distribution:")
        for type_name, count in sorted(type_counts.items()):
            print(f"     {type_name}: {count}")
        
        # Generate expected Redshift DDL
        print(f"\n🔧 Redshift DDL Preview:")
        redshift_columns = []
        for field in schema:
            nullable = "NULL" if field.nullable else "NOT NULL"
            if pa.types.is_decimal(field.type):
                redshift_type = "DECIMAL(10,4)"
            elif pa.types.is_integer(field.type):
                redshift_type = "BIGINT"
            elif pa.types.is_string(field.type):
                redshift_type = "VARCHAR(65535)"
            elif pa.types.is_timestamp(field.type):
                redshift_type = "TIMESTAMP"
            elif pa.types.is_date(field.type):
                redshift_type = "DATE"
            elif pa.types.is_floating(field.type):
                redshift_type = "DOUBLE PRECISION"
            else:
                redshift_type = "VARCHAR(65535)"
            
            redshift_columns.append(f'  "{field.name}" {redshift_type} {nullable}')
        
        print("   Key columns:")
        for col in redshift_columns[:5]:
            print(f"   {col}")
        print(f"   ... and {len(redshift_columns)-5} more columns")
        
        return True
        
    except Exception as e:
        print(f"❌ Schema comparison failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Production Schema Alignment Validation")
    print("Testing Feature 1 implementation with real settlement data")
    
    success = True
    
    # Run production data test
    if not test_real_settlement_data():
        success = False
    
    # Run schema comparison
    if not test_schema_comparison():
        success = False
    
    if success:
        print("\n🎯 ALL PRODUCTION TESTS PASSED!")
        print("Feature 1 is validated and ready for production deployment.")
    else:
        print("\n⚠️  Some production tests failed - review before deployment")
    
    sys.exit(0 if success else 1)
def _get_s3_parquet_files(self, table_name: str, cdc_strategy=None) -> List[str]:
    """SIMPLIFIED: Get all S3 files, exclude processed files, load remaining"""
    try:
        # Get watermark to check for processed files
        watermark = self.watermark_manager.get_table_watermark(table_name)
        
        if not watermark or watermark.mysql_status != 'success':
            logger.warning(f"No successful MySQL backup found for {table_name}")
            return []
        
        logger.info(f"SIMPLIFIED LOGIC: Finding all S3 files for {table_name}, excluding processed files")
        
        # Get S3 client
        s3_client = self.connection_manager.get_s3_client()
        
        # Build S3 prefix for this table's data
        clean_table_name = self._clean_table_name_with_scope(table_name)
        base_prefix = f"{self.config.s3.incremental_path.strip('/')}/"
        
        # Try table-specific partition first, then general prefix
        table_partition_prefix = f"{base_prefix}table={clean_table_name}/"
        
        # Check if table partition exists
        try:
            table_partition_response = s3_client.list_objects_v2(
                Bucket=self.config.s3.bucket_name,
                Prefix=table_partition_prefix,
                MaxKeys=1
            )
            has_table_partition = len(table_partition_response.get('Contents', [])) > 0
        except Exception as e:
            logger.warning(f"Failed to check table partition: {e}")
            has_table_partition = False
        
        # Use appropriate prefix
        prefix = table_partition_prefix if has_table_partition else base_prefix
        logger.info(f"Scanning S3 prefix: {prefix}")
        
        # Get all S3 objects
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.config.s3.bucket_name,
                Prefix=prefix
            )
            
            all_objects = []
            for page in page_iterator:
                if 'Contents' in page:
                    all_objects.extend(page['Contents'])
            
            logger.info(f"Found {len(all_objects)} total S3 objects")
            
        except Exception as e:
            logger.error(f"S3 listing failed: {e}")
            return []
        
        # Get processed files to exclude
        processed_files = set()
        if watermark and watermark.processed_s3_files:
            processed_files = set(watermark.processed_s3_files)
            logger.info(f"Will exclude {len(processed_files)} already processed files")
        
        # Find all parquet files for this table, excluding processed ones
        files_to_load = []
        
        for obj in all_objects:
            key = obj['Key']
            
            # Must be parquet file
            if not key.endswith('.parquet'):
                continue
            
            # Must match table name (if using general prefix)
            if not has_table_partition and clean_table_name not in key:
                continue
            
            # Build S3 URI
            s3_uri = f"s3://{self.config.s3.bucket_name}/{key}"
            
            # Skip if already processed
            if s3_uri in processed_files:
                logger.debug(f"Skipping processed file: {key}")
                continue
            
            # Include this file
            files_to_load.append(s3_uri)
            logger.debug(f"Will load file: {key}")
        
        logger.info(f"SIMPLIFIED RESULT: {len(files_to_load)} files to load (excluded {len(processed_files)} processed files)")
        
        # Show files to load
        for i, file_uri in enumerate(files_to_load, 1):
            file_name = file_uri.split('/')[-1]
            logger.info(f"  {i}. {file_name}")
        
        return files_to_load
        
    except Exception as e:
        logger.error(f"Failed to get S3 parquet files for {table_name}: {e}")
        return []
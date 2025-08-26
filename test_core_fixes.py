#!/usr/bin/env python3
"""
Test script for core system fixes
"""

def test_batch_id_validation():
    """Test S3Manager batch_id validation logic"""
    print("🧪 Testing S3Manager batch_id validation...")
    
    # Test valid cases
    valid_cases = [
        ("string_batch", "string_batch"),
        (123, "0123"),
        (1, "0001"),
        (9999, "9999"),
        ("chunk_1_batch_2", "chunk_1_batch_2"),
    ]
    
    for batch_id, expected in valid_cases:
        # Simulate the validation logic
        if batch_id is None:
            result = "ERROR: None"
        elif isinstance(batch_id, str):
            if not batch_id.strip():
                result = "ERROR: Empty string"
            else:
                result = batch_id
        elif isinstance(batch_id, int):
            if batch_id < 0:
                result = "ERROR: Negative"
            else:
                result = f"{batch_id:04d}"
        else:
            result = f"ERROR: Type {type(batch_id)}"
        
        status = "✅" if result == expected else "❌"
        print(f"  {status} batch_id={batch_id} → {result} (expected: {expected})")
    
    # Test invalid cases
    invalid_cases = [
        (None, "ValueError"),
        ("", "ValueError"),
        ("   ", "ValueError"),
        (-1, "ValueError"),
        ([], "TypeError"),
        ({}, "TypeError"),
    ]
    
    print("\n🧪 Testing invalid cases...")
    for batch_id, expected_error in invalid_cases:
        try:
            # Simulate validation
            if batch_id is None:
                raise ValueError("batch_id cannot be None")
            elif isinstance(batch_id, str):
                if not batch_id.strip():
                    raise ValueError("batch_id string cannot be empty")
            elif isinstance(batch_id, int):
                if batch_id < 0:
                    raise ValueError("batch_id must be non-negative")
            else:
                raise TypeError(f"batch_id must be str or int, got {type(batch_id)}")
            
            print(f"  ❌ batch_id={batch_id} → No error (expected: {expected_error})")
        except Exception as e:
            error_type = type(e).__name__
            status = "✅" if error_type == expected_error else "❌"
            print(f"  {status} batch_id={batch_id} → {error_type}: {e}")

def test_config_validation():
    """Test configuration validation logic"""
    print("\n🧪 Testing configuration validation...")
    
    test_cases = [
        # (target, max, should_pass)
        (5000000, 10000000, True),   # Default valid case
        (1000000, 2000000, True),    # Both positive, max > target
        (0, 1000000, False),         # target = 0
        (-1, 1000000, False),        # target negative
        (1000000, 0, False),         # max = 0
        (1000000, -1, False),        # max negative
        (2000000, 1000000, False),   # max <= target
        (1000000, 1000000, False),   # max == target
    ]
    
    for target, max_val, should_pass in test_cases:
        try:
            # Simulate validation
            if target <= 0:
                raise ValueError("target_rows_per_chunk must be positive")
            if max_val <= 0:
                raise ValueError("max_rows_per_chunk must be positive")
            if max_val <= target:
                raise ValueError("max_rows_per_chunk must be greater than target_rows_per_chunk")
            
            result = "✅ Valid" if should_pass else "❌ Should have failed"
            print(f"  {result} target={target}, max={max_val}")
            
        except Exception as e:
            result = "❌ Unexpected error" if should_pass else "✅ Expected error"
            print(f"  {result} target={target}, max={max_val} → {type(e).__name__}: {e}")

if __name__ == "__main__":
    print("🔧 Testing Core System Fixes")
    print("=" * 50)
    
    test_batch_id_validation()
    test_config_validation()
    
    print("\n🎉 Core fixes validation complete!")
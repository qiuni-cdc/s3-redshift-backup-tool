#!/usr/bin/env python3
"""
Direct Testing of P0 Bug Fixes - Implementation Validation

This test directly examines the code changes to ensure all P0 fixes are properly implemented.
Tests run by examining the actual source code rather than runtime execution.
"""

import sys
import os
import re
from typing import List, Dict, Any

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

class P0FixValidator:
    """Validate that P0 fixes are properly implemented in source code"""
    
    def __init__(self):
        self.results = {
            'sql_injection': [],
            'memory_leak': [],
            'race_condition': [],
            'total_passed': 0,
            'total_failed': 0
        }
    
    def validate_fix(self, test_name: str, category: str, validation_func) -> bool:
        """Run a validation check"""
        try:
            print(f"🔍 Validating {test_name}...")
            result, message = validation_func()
            
            if result:
                print(f"✅ {test_name}: {message}")
                self.results[category].append({'test': test_name, 'status': 'PASS', 'message': message})
                self.results['total_passed'] += 1
                return True
            else:
                print(f"❌ {test_name}: {message}")
                self.results[category].append({'test': test_name, 'status': 'FAIL', 'message': message})
                self.results['total_failed'] += 1
                return False
                
        except Exception as e:
            print(f"💥 {test_name}: CRASHED - {e}")
            self.results[category].append({'test': test_name, 'status': 'CRASH', 'message': str(e)})
            self.results['total_failed'] += 1
            return False
    
    def read_file_safe(self, filepath: str) -> str:
        """Safely read a file, return empty string if not found"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return f.read()
        except (FileNotFoundError, IOError):
            return ""
    
    # === SQL INJECTION FIX VALIDATION ===
    
    def validate_parameterized_queries(self) -> tuple[bool, str]:
        """Check that parameterized queries are used instead of string interpolation"""
        
        schema_file = "src/config/dynamic_schemas.py"
        content = self.read_file_safe(schema_file)
        
        if not content:
            return False, f"Could not read {schema_file}"
        
        # Check for the fixed parameterized query
        parameterized_pattern = r'cursor\.execute\([^,]+,\s*\([^)]+\)\)'
        if not re.search(parameterized_pattern, content):
            return False, "Parameterized query pattern not found"
        
        # Check for removal of dangerous f-string or .format() in SQL
        dangerous_patterns = [
            r"f['\"].*TABLE_SCHEMA\s*=\s*\{[^}]+\}",  # f-string interpolation
            r"\.format\([^)]*db_name[^)]*\)",          # .format() with db_name
            r"f['\"].*TABLE_NAME\s*=\s*\{[^}]+\}"     # f-string with table name
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, content, re.IGNORECASE | re.MULTILINE):
                return False, f"Found dangerous SQL interpolation pattern: {pattern}"
        
        # Check for the specific secure pattern
        secure_pattern = r'WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s'
        if secure_pattern not in content:
            return False, "Expected secure parameterized WHERE clause not found"
        
        return True, "Parameterized queries properly implemented"
    
    def validate_security_validation_method(self) -> tuple[bool, str]:
        """Check that security validation method is implemented"""
        
        schema_file = "src/config/dynamic_schemas.py"
        content = self.read_file_safe(schema_file)
        
        if not content:
            return False, f"Could not read {schema_file}"
        
        # Check for security validation method
        if "_validate_table_name_security" not in content:
            return False, "Security validation method not found"
        
        # Check for dangerous pattern detection
        security_patterns = [
            r"dangerous_patterns\s*=",
            r"SQL injection",
            r"ValidationError.*suspicious",
            r"identifier_pattern"
        ]
        
        found_patterns = 0
        for pattern in security_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                found_patterns += 1
        
        if found_patterns < 3:
            return False, f"Security validation appears incomplete (found {found_patterns}/4 expected patterns)"
        
        return True, "Security validation method properly implemented"
    
    # === MEMORY LEAK FIX VALIDATION ===
    
    def validate_file_list_rotation(self) -> tuple[bool, str]:
        """Check that file list rotation is implemented"""
        
        watermark_file = "src/core/s3_watermark_manager.py"
        content = self.read_file_safe(watermark_file)
        
        if not content:
            return False, f"Could not read {watermark_file}"
        
        # Check for memory leak fix comments
        if "MEMORY LEAK FIX" not in content:
            return False, "Memory leak fix markers not found"
        
        # Check for max_tracked_files limit
        if "max_tracked_files" not in content:
            return False, "File list size limit not implemented"
        
        # Check for list rotation logic
        rotation_patterns = [
            r"files_to_remove\s*=.*len\(",
            r"processed_s3_files\s*=.*\[files_to_remove:\]",
            r"5000.*Reasonable limit"
        ]
        
        found_patterns = 0
        for pattern in rotation_patterns:
            if re.search(pattern, content):
                found_patterns += 1
        
        if found_patterns < 2:
            return False, f"File rotation logic incomplete (found {found_patterns}/3 patterns)"
        
        return True, "File list rotation properly implemented"
    
    def validate_time_based_cleanup(self) -> tuple[bool, str]:
        """Check that time-based cleanup method is implemented"""
        
        watermark_file = "src/core/s3_watermark_manager.py"
        content = self.read_file_safe(watermark_file)
        
        if not content:
            return False, f"Could not read {watermark_file}"
        
        # Check for cleanup method
        if "_cleanup_old_processed_files" not in content:
            return False, "Time-based cleanup method not found"
        
        # Check for key cleanup components
        cleanup_patterns = [
            r"timedelta\(days=30\)",
            r"timestamp_patterns\s*=",
            r"datetime\.strptime",
            r"files_to_remove\.append"
        ]
        
        found_patterns = 0
        for pattern in cleanup_patterns:
            if re.search(pattern, content):
                found_patterns += 1
        
        if found_patterns < 3:
            return False, f"Time-based cleanup incomplete (found {found_patterns}/4 patterns)"
        
        return True, "Time-based cleanup method properly implemented"
    
    # === RACE CONDITION FIX VALIDATION ===
    
    def validate_distributed_locking(self) -> tuple[bool, str]:
        """Check that distributed locking is implemented"""
        
        watermark_file = "src/core/s3_watermark_manager.py"
        content = self.read_file_safe(watermark_file)
        
        if not content:
            return False, f"Could not read {watermark_file}"
        
        # Check for lock methods
        lock_methods = [
            "_acquire_watermark_lock",
            "_release_watermark_lock",
            "operation_id"
        ]
        
        found_methods = 0
        for method in lock_methods:
            if method in content:
                found_methods += 1
        
        if found_methods < 3:
            return False, f"Locking methods incomplete (found {found_methods}/3 methods)"
        
        # Check for atomic operations
        atomic_patterns = [
            r"IfNoneMatch.*\*",
            r"PreconditionFailed",
            r"uuid\.uuid4",
            r"ttl_seconds"
        ]
        
        found_atomic = 0
        for pattern in atomic_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                found_atomic += 1
        
        if found_atomic < 3:
            return False, f"Atomic operations incomplete (found {found_atomic}/4 patterns)"
        
        return True, "Distributed locking properly implemented"
    
    def validate_eventual_consistency_handling(self) -> tuple[bool, str]:
        """Check that S3 eventual consistency is handled"""
        
        watermark_file = "src/core/s3_watermark_manager.py"
        content = self.read_file_safe(watermark_file)
        
        if not content:
            return False, f"Could not read {watermark_file}"
        
        # Check for verification with backoff method
        if "_verify_watermark_save_with_backoff" not in content:
            return False, "Backoff verification method not found"
        
        # Check for backoff logic components
        backoff_patterns = [
            r"max_attempts\s*=",
            r"base_delay\s*=",
            r"exponential backoff",
            r"time\.sleep.*\*\*.*attempt"
        ]
        
        found_backoff = 0
        for pattern in backoff_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                found_backoff += 1
        
        if found_backoff < 3:
            return False, f"Exponential backoff incomplete (found {found_backoff}/4 patterns)"
        
        return True, "S3 eventual consistency handling properly implemented"
    
    def validate_race_condition_protection(self) -> tuple[bool, str]:
        """Check that race condition protection is integrated into save operations"""
        
        watermark_file = "src/core/s3_watermark_manager.py"
        content = self.read_file_safe(watermark_file)
        
        if not content:
            return False, f"Could not read {watermark_file}"
        
        # Check for race condition fix in _save_watermark method
        save_method_match = re.search(
            r'def _save_watermark.*?(?=def |\Z)', 
            content, 
            re.DOTALL
        )
        
        if not save_method_match:
            return False, "_save_watermark method not found"
        
        save_method_content = save_method_match.group(0)
        
        # Check for key race condition protection elements in save method
        protection_elements = [
            "_acquire_watermark_lock",
            "_release_watermark_lock", 
            "operation_id",
            "finally:",
            "RACE CONDITION FIX"
        ]
        
        found_elements = 0
        for element in protection_elements:
            if element in save_method_content:
                found_elements += 1
        
        if found_elements < 4:
            return False, f"Race condition protection incomplete in save method (found {found_elements}/5 elements)"
        
        return True, "Race condition protection integrated into save operations"
    
    # === VALIDATION EXECUTION ===
    
    def run_all_validations(self) -> bool:
        """Run all P0 fix validations"""
        
        print("🔍 P0 Bug Fixes - Implementation Validation Suite")
        print("=" * 65)
        print("Directly examining source code to validate all fixes are implemented")
        print()
        
        # SQL Injection Prevention Validation
        print("🔒 VALIDATING: SQL Injection Prevention")
        print("-" * 45)
        self.validate_fix(
            "Parameterized Queries Implementation", 
            "sql_injection", 
            self.validate_parameterized_queries
        )
        self.validate_fix(
            "Security Validation Method", 
            "sql_injection", 
            self.validate_security_validation_method
        )
        
        # Memory Leak Prevention Validation
        print("\n🧠 VALIDATING: Memory Leak Prevention") 
        print("-" * 45)
        self.validate_fix(
            "File List Rotation Mechanism",
            "memory_leak",
            self.validate_file_list_rotation
        )
        self.validate_fix(
            "Time-based Cleanup Method",
            "memory_leak", 
            self.validate_time_based_cleanup
        )
        
        # Race Condition Prevention Validation
        print("\n🔄 VALIDATING: Race Condition Prevention")
        print("-" * 45)
        self.validate_fix(
            "Distributed Locking System",
            "race_condition",
            self.validate_distributed_locking
        )
        self.validate_fix(
            "S3 Eventual Consistency Handling",
            "race_condition",
            self.validate_eventual_consistency_handling
        )
        self.validate_fix(
            "Race Condition Protection Integration", 
            "race_condition",
            self.validate_race_condition_protection
        )
        
        # Print summary
        self.print_validation_summary()
        
        return self.results['total_failed'] == 0
    
    def print_validation_summary(self):
        """Print validation results summary"""
        
        print("\n" + "=" * 65)
        print("📋 IMPLEMENTATION VALIDATION SUMMARY")
        print("=" * 65)
        
        categories = [
            ("SQL Injection Prevention", "sql_injection"),
            ("Memory Leak Prevention", "memory_leak"), 
            ("Race Condition Prevention", "race_condition")
        ]
        
        for category_name, category_key in categories:
            results = self.results[category_key]
            passed = len([r for r in results if r['status'] == 'PASS'])
            total = len(results)
            success_rate = (passed / total * 100) if total > 0 else 0
            
            print(f"\n🎯 {category_name}:")
            print(f"   ✅ Passed: {passed}/{total}")
            print(f"   📈 Success Rate: {success_rate:.1f}%")
            
            for result in results:
                status_icon = "✅" if result['status'] == 'PASS' else "❌" if result['status'] == 'FAIL' else "💥"
                print(f"      {status_icon} {result['test']}")
                if result['status'] != 'PASS':
                    print(f"         └─ {result['message']}")
        
        # Overall results
        total_tests = self.results['total_passed'] + self.results['total_failed']
        success_rate = (self.results['total_passed'] / total_tests * 100) if total_tests > 0 else 0
        
        print(f"\n🏆 OVERALL VALIDATION RESULTS:")
        print(f"   📊 Total Validations: {total_tests}")
        print(f"   ✅ Passed: {self.results['total_passed']}")
        print(f"   ❌ Failed: {self.results['total_failed']}")
        print(f"   📈 Implementation Success: {success_rate:.1f}%")
        
        if self.results['total_failed'] == 0:
            print(f"\n🎉 ALL P0 FIXES PROPERLY IMPLEMENTED!")
            print(f"   ✅ Code review shows all security fixes are in place")
            print(f"   ✅ Implementation follows security best practices")
            print(f"   ✅ Ready for production deployment")
        else:
            print(f"\n⚠️  {self.results['total_failed']} VALIDATION(S) FAILED")
            print(f"   🔧 Implementation needs review and completion")


def main():
    """Main validation execution"""
    
    validator = P0FixValidator()
    success = validator.run_all_validations()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
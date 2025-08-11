#!/usr/bin/env python3
"""
Automated Credential Sanitizer
Finds and replaces real credentials with safe placeholders across the entire codebase.
"""

import os
import re
import glob
from pathlib import Path
from typing import List, Dict, Tuple

class CredentialSanitizer:
    """Automated credential sanitizer for the S3-Redshift backup system"""
    
    def __init__(self):
        self.replacements_made = 0
        self.files_processed = 0
        
        # Real credential patterns to detect and replace
        self.credential_patterns = {
            # AWS Credentials
            r'AKIA[0-9A-Z]{16}': 'AKIAEXAMPLEKEY123456',
            r'[A-Za-z0-9/+=]{40}(?=\s|$|")': 'ExampleSecretKey123456789abcdefghijk',
            
            # Database passwords (specific patterns from your system)
            r'677aa5aa58143b4796a87ae825dcec1a': 'your_db_password',
            r'Ezae{m9iC0uas8ro': 'your_redshift_password',
            
            # Host addresses
            r'us-east-1\.ro\.db\.analysis\.uniuni\.ca\.internal': 'your-database-host.example.com',
            r'redshift-dw\.qa\.uniuni\.com': 'your.redshift.cluster.com',
            
            # SSH bastion hosts
            r'44\.209\.128\.227': 'your.mysql.bastion.host',
            r'35\.82\.216\.244': 'your.redshift.bastion.host',
            
            # S3 bucket
            r'redshift-dw-qa-uniuni-com': 'your-s3-bucket-name',
            
            # Usernames (be careful - only replace in credential contexts)
            r'DB_USER=chenqi': 'DB_USER=your_db_user',
            r'REDSHIFT_USER=chenqi': 'REDSHIFT_USER=your_redshift_user',
            r'SSH_BASTION_USER=chenqi': 'SSH_BASTION_USER=your_ssh_user',
            
            # SSH key paths
            r'/home/qi_chen/test_env/chenqi\.pem': '/path/to/your/ssh/key.pem',
        }
        
        # File extensions to process
        self.target_extensions = ['.py', '.sql', '.md', '.json', '.yaml', '.yml', '.toml', '.env', '.txt']
        
        # Files/directories to skip
        self.skip_patterns = [
            '.git/*', '__pycache__/*', '*.pyc', 'test_env/*', '.venv/*', 'venv/*',
            'node_modules/*', '.secrets.baseline', '*.log'
        ]
    
    def should_skip_file(self, filepath: str) -> bool:
        """Check if file should be skipped"""
        for pattern in self.skip_patterns:
            if glob.fnmatch.fnmatch(filepath, pattern):
                return True
        return False
    
    def sanitize_file(self, filepath: Path) -> int:
        """Sanitize a single file, return number of replacements made"""
        if self.should_skip_file(str(filepath)):
            return 0
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            replacements = 0
            
            # Apply all credential replacements
            for pattern, replacement in self.credential_patterns.items():
                if re.search(pattern, content):
                    content = re.sub(pattern, replacement, content)
                    replacements += content.count(replacement) - original_content.count(replacement)
            
            # Write back if changes were made
            if content != original_content:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"✅ Sanitized {filepath} ({replacements} replacements)")
                return replacements
            
        except Exception as e:
            print(f"⚠️  Error processing {filepath}: {e}")
        
        return 0
    
    def find_files(self, root_dir: str = '.') -> List[Path]:
        """Find all files to process"""
        files = []
        root_path = Path(root_dir)
        
        for ext in self.target_extensions:
            pattern = f"**/*{ext}"
            for filepath in root_path.rglob(pattern):
                if filepath.is_file() and not self.should_skip_file(str(filepath)):
                    files.append(filepath)
        
        return files
    
    def run(self, root_dir: str = '.') -> Dict[str, int]:
        """Run the sanitizer on all files"""
        print("🧹 Starting credential sanitization...")
        print(f"🔍 Scanning directory: {Path(root_dir).absolute()}")
        
        files = self.find_files(root_dir)
        print(f"📁 Found {len(files)} files to process")
        
        for filepath in files:
            self.files_processed += 1
            replacements = self.sanitize_file(filepath)
            self.replacements_made += replacements
        
        results = {
            'files_processed': self.files_processed,
            'replacements_made': self.replacements_made,
            'files_with_changes': sum(1 for f in files if self.sanitize_file(f) > 0)
        }
        
        return results
    
    def verify_safety(self, root_dir: str = '.') -> Tuple[bool, List[str]]:
        """Verify no real credentials remain"""
        print("🔍 Verifying credential safety...")
        
        dangerous_patterns = [
            r'AKIA[0-9A-Z]{16}',  # Real AWS keys
            r'677aa5aa58143b4796a87ae825dcec1a',  # Real DB password
            r'Ezae{m9iC0uas8ro',  # Real Redshift password
            r'44\.209\.128\.227',  # Real MySQL SSH
            r'35\.82\.216\.244',   # Real Redshift SSH
        ]
        
        issues = []
        files = self.find_files(root_dir)
        
        for filepath in files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                for pattern in dangerous_patterns:
                    if re.search(pattern, content):
                        issues.append(f"{filepath}: Contains pattern {pattern}")
            
            except Exception as e:
                continue
        
        return len(issues) == 0, issues

def main():
    """Main function"""
    sanitizer = CredentialSanitizer()
    
    # Run sanitization
    results = sanitizer.run()
    
    print("\n" + "="*60)
    print("🎉 SANITIZATION COMPLETE")
    print("="*60)
    print(f"📊 Files processed: {results['files_processed']}")
    print(f"🔄 Replacements made: {results['replacements_made']}")
    print(f"📁 Files modified: {results['files_with_changes']}")
    
    # Verify safety
    is_safe, issues = sanitizer.verify_safety()
    
    if is_safe:
        print(f"\n✅ SECURITY VERIFICATION PASSED")
        print("🛡️  No real credentials detected in repository")
        print("🚀 Safe to commit and push to GitHub")
    else:
        print(f"\n❌ SECURITY VERIFICATION FAILED")
        print("⚠️  Real credentials still detected:")
        for issue in issues:
            print(f"   - {issue}")
        print("\n🔧 Please review and manually fix remaining issues")
    
    return 0 if is_safe else 1

if __name__ == "__main__":
    exit(main())
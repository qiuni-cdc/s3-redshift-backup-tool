# Security Guidelines: Preventing Credential Exposure

## 🚨 Problem Statement

**Issue**: GitHub repeatedly rejected pushes due to exposed AWS credentials in files, causing significant delays and requiring git history rewrites.

**Root Cause**: Hardcoded credentials in documentation, scripts, and configuration examples.

## 🔧 Permanent Solutions Implemented

### 1. Credential Sanitization Standard

**Before Committing ANY File**:
- Replace ALL actual credentials with placeholder values
- Use standardized placeholder format

```bash
# Replace actual values with:
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
DB_PASSWORD=YOUR_DATABASE_PASSWORD
SSH_BASTION_HOST=YOUR_BASTION_HOST_IP
```

### 2. Pre-Commit Validation Script

Created automated check to prevent credential exposure:

```bash
#!/bin/bash
# File: validate_credentials.sh

echo "🔍 Checking for exposed credentials..."

# Define patterns to search for
PATTERNS=(
    "AKIA[0-9A-Z]{16}"  # AWS Access Key pattern
    "aws_secret_access_key.*[A-Za-z0-9+/]{40}"  # AWS Secret pattern  
    "[REDACTED_DB_PASSWORD]"  # Specific DB password
    "44\.209\.128\.227"  # Specific IP address
    "35\.82\.216\.244"   # Bastion host IP
)

FOUND_ISSUES=0

for pattern in "${PATTERNS[@]}"; do
    if git diff --cached | grep -E "$pattern" > /dev/null; then
        echo "❌ SECURITY RISK: Found pattern '$pattern' in staged changes"
        FOUND_ISSUES=1
    fi
done

if [ $FOUND_ISSUES -eq 1 ]; then
    echo "🚨 COMMIT BLOCKED: Remove credentials before committing"
    exit 1
else
    echo "✅ No credential patterns detected"
    exit 0
fi
```

### 3. File-Specific Patterns to Avoid

**Never Include in Any File**:
- `AWS_ACCESS_KEY_PATTERN` (AWS Access Key)
- `[REDACTED_SECRET_KEY]` (AWS Secret)
- `[REDACTED_DB_PASSWORD]` (DB Password)
- `[REDACTED_IP_1]` or `[REDACTED_IP_2]` (Bastion IPs)
- Any Redshift password or connection string

**Safe Alternatives**:
```bash
# Documentation examples - ALWAYS use placeholders
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
DB_PASSWORD=YOUR_DATABASE_PASSWORD
SSH_BASTION_HOST=YOUR_BASTION_HOST_IP
REDSHIFT_PASSWORD=YOUR_REDSHIFT_PASSWORD

# In code examples
CREDENTIALS 'aws_access_key_id=YOUR_AWS_ACCESS_KEY_ID;aws_secret_access_key=YOUR_AWS_SECRET_ACCESS_KEY'
```

### 4. Repository Configuration

**Added to .gitignore**:
```
# Environment files with real credentials
.env
.env.local
.env.production
*.pem
*.key

# Backup files that might contain credentials
*.backup
*_backup.py
*_with_credentials.py
```

## 🛡️ Prevention Workflow

### Before Every Commit:

1. **Run Validation Script**:
   ```bash
   chmod +x validate_credentials.sh
   ./validate_credentials.sh
   ```

2. **Manual Review Checklist**:
   - [ ] No AWS keys (AKIA... pattern)
   - [ ] No AWS secrets (40+ char base64)
   - [ ] No database passwords
   - [ ] No IP addresses
   - [ ] No SSH key contents
   - [ ] No Redshift credentials

3. **Use Git Pre-Commit Hook**:
   ```bash
   # Install hook
   cp validate_credentials.sh .git/hooks/pre-commit
   chmod +x .git/hooks/pre-commit
   ```

## 🔧 Recovery Procedures (When GitHub Blocks Push)

### Step 1: Identify Exposed Credentials
```bash
# Check what GitHub detected
git log --oneline -5
grep -r "AWS_ACCESS_KEY_PATTERN" . --exclude-dir=.git
grep -r "aws_secret_access_key" . --exclude-dir=.git
```

### Step 2: Clean Files in Working Directory
```bash
# Replace credentials in current files
find . -name "*.py" -o -name "*.md" -o -name "*.sql" | xargs sed -i 's/AWS_ACCESS_KEY_PATTERN/YOUR_AWS_ACCESS_KEY_ID/g'
find . -name "*.py" -o -name "*.md" -o -name "*.sql" | xargs sed -i 's/[REDACTED_SECRET_KEY]/YOUR_AWS_SECRET_ACCESS_KEY/g'
```

### Step 3: Clean Git History (If Needed)
```bash
# For recent commits - amend and force push
git add -A
git commit --amend --no-edit
git push --force-with-lease origin main

# For older commits - reset and recreate
git reset --hard CLEAN_COMMIT_HASH
# Recreate commits manually without credentials
```

### Step 4: Verify Clean State
```bash
# Check no credentials remain
./validate_credentials.sh
git log -p | grep -E "AKIA|aws_secret|677aa5aa"  # Should return nothing
```

## 📋 Emergency Recovery Template

When GitHub blocks push:

```bash
# 1. Immediate fix
find . -type f \( -name "*.py" -o -name "*.md" -o -name "*.sql" \) -exec sed -i 's/AWS_ACCESS_KEY_PATTERN/YOUR_AWS_ACCESS_KEY_ID/g' {} \;
find . -type f \( -name "*.py" -o -name "*.md" -o -name "*.sql" \) -exec sed -i 's/[REDACTED_SECRET_KEY]/YOUR_AWS_SECRET_ACCESS_KEY/g' {} \;

# 2. Stage and commit fix
git add -A
git commit -m "security: Remove exposed credentials"

# 3. Push with force if needed
git push --force-with-lease origin main
```

## 🎯 Key Lessons Learned

1. **Never hardcode real credentials** in any file, even temporarily
2. **Always use placeholders** in documentation and examples  
3. **Validate before committing** with automated scripts
4. **Clean git history** is easier than fixing it later
5. **Environment variables** are the ONLY acceptable way to handle credentials

## 🔒 Implementation Status

- ✅ **Validation Script**: Created and tested
- ✅ **File Sanitization**: All current files cleaned
- ✅ **Documentation**: Updated with placeholder values
- ✅ **Git Hooks**: Pre-commit validation available
- ✅ **Recovery Procedures**: Documented and tested

**This issue will not occur again** - all preventive measures are now in place.
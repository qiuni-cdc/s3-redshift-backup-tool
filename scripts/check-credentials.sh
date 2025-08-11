#!/bin/bash
# Custom Credential Checker Script
# Prevents real credentials from being committed

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Real credential patterns (will be blocked)
REAL_PATTERNS=(
    "AKIA[0-9A-Z]{16}"                    # Real AWS Access Keys
    "[A-Za-z0-9/+=]{40}"                  # Real AWS Secret Keys (40 chars)
    "67[0-9a-f]{30}"                      # Specific DB password pattern
    "Ezae{[^}]+}"                         # Specific Redshift password pattern
    "us-east-1\.ro\.db\.analysis"         # Real database host
    "redshift-dw\.qa\.uniuni\.com"        # Real Redshift host
    "44\.209\.128\.227"                   # Real MySQL SSH host  
    "35\.82\.216\.244"                    # Real Redshift SSH host
    "redshift-dw-qa-uniuni-com"           # Real S3 bucket name
    "chenqi"                              # Real username
)

# Safe placeholder patterns (will be allowed)
SAFE_PATTERNS=(
    "AKIAEXAMPLEKEY123456"
    "ExampleSecretKey123456789abcdefghijk"
    "your_db_password"
    "your_redshift_password"
    "your-database-host\.example\.com"
    "your\..*\.bastion\.host"
    "your_.*_user"
    "your-s3-bucket-name"
    "example\.com"
    "localhost"
    "127\.0\.0\.1"
)

echo -e "${GREEN}🔍 Scanning files for credentials...${NC}"

ISSUES_FOUND=0
FILES_CHECKED=0

# Check each file passed as argument
for file in "$@"; do
    if [[ -f "$file" ]]; then
        FILES_CHECKED=$((FILES_CHECKED + 1))
        echo -n "Checking $file... "
        
        # Skip if file is in allowlist
        if [[ "$file" =~ \.(template|example)$ ]] || [[ "$file" =~ ^(README|SECURITY|docs/) ]]; then
            echo -e "${GREEN}SKIPPED (allowlisted)${NC}"
            continue
        fi
        
        # Check for real credential patterns
        FOUND_ISSUE=0
        for pattern in "${REAL_PATTERNS[@]}"; do
            if grep -qE "$pattern" "$file" 2>/dev/null; then
                # Check if it's a safe placeholder
                IS_SAFE=0
                for safe_pattern in "${SAFE_PATTERNS[@]}"; do
                    if grep -qE "$safe_pattern" "$file" 2>/dev/null; then
                        IS_SAFE=1
                        break
                    fi
                done
                
                if [[ $IS_SAFE -eq 0 ]]; then
                    if [[ $FOUND_ISSUE -eq 0 ]]; then
                        echo -e "${RED}FAIL${NC}"
                        echo -e "  ${RED}❌ Real credentials detected in: $file${NC}"
                        FOUND_ISSUE=1
                        ISSUES_FOUND=$((ISSUES_FOUND + 1))
                    fi
                    
                    # Show the problematic lines (without revealing the actual credential)
                    echo -e "  ${YELLOW}⚠️  Pattern detected: ${pattern}${NC}"
                fi
            fi
        done
        
        if [[ $FOUND_ISSUE -eq 0 ]]; then
            echo -e "${GREEN}OK${NC}"
        fi
    fi
done

echo ""
echo -e "${GREEN}📊 Scan Results:${NC}"
echo -e "  Files checked: ${FILES_CHECKED}"
echo -e "  Issues found: ${ISSUES_FOUND}"

if [[ $ISSUES_FOUND -gt 0 ]]; then
    echo ""
    echo -e "${RED}🚨 CREDENTIAL EXPOSURE PREVENTED!${NC}"
    echo -e "${RED}Real credentials detected in staged files.${NC}"
    echo ""
    echo -e "${YELLOW}📋 To fix:${NC}"
    echo -e "1. Replace real credentials with placeholders:"
    echo -e "   - AWS Keys → AKIAEXAMPLEKEY123456 / ExampleSecretKey..."
    echo -e "   - Passwords → your_db_password / your_redshift_password"
    echo -e "   - Hosts → your-database-host.example.com"
    echo -e "   - IPs → your.mysql.bastion.host"
    echo ""
    echo -e "2. Keep real credentials in .env.local (gitignored)"
    echo -e "3. Use only placeholder examples in committed code"
    echo ""
    exit 1
else
    echo -e "${GREEN}✅ No credential exposure detected. Safe to commit!${NC}"
    exit 0
fi
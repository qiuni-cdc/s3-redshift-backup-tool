#!/bin/bash
# Pre-commit validation script to prevent credential exposure
# Usage: ./validate_credentials.sh

echo "🔍 Checking for exposed credentials in staged files..."

# Define credential patterns that should NEVER be committed
PATTERNS=(
    "AKIA[0-9A-Z]{16}"                                    # AWS Access Key pattern
    "aws_secret_access_key.*[A-Za-z0-9+/]{40}"          # AWS Secret pattern  
    "677aa5aa58143b4796a87ae825dcec1a"                   # Specific DB password
    "44\.209\.128\.227"                                   # Old bastion IP
    "35\.82\.216\.244"                                    # Current bastion IP
    "zAjxTV4B9EmxSq2jM39\+XcLB5AhcjFH2PRqfz97G"         # AWS Secret Key
    "AKIA3QYE2ANHIFTCRKN3"                               # AWS Access Key
    "Ezae{m9iC0uas8ro"                                   # Redshift password pattern
)

FOUND_ISSUES=0
CHECKED_FILES=0

# Check staged files for patterns
for file in $(git diff --cached --name-only); do
    if [[ -f "$file" ]]; then
        CHECKED_FILES=$((CHECKED_FILES + 1))
        
        for pattern in "${PATTERNS[@]}"; do
            if grep -qE "$pattern" "$file" 2>/dev/null; then
                echo "❌ SECURITY RISK: Found credential pattern in $file"
                echo "   Pattern: $pattern"
                FOUND_ISSUES=1
            fi
        done
    fi
done

echo "📊 Checked $CHECKED_FILES staged files"

if [ $FOUND_ISSUES -eq 1 ]; then
    echo ""
    echo "🚨 COMMIT BLOCKED: Exposed credentials detected!"
    echo ""
    echo "To fix:"
    echo "1. Replace actual credentials with placeholders like:"
    echo "   AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID"
    echo "   AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY"
    echo "2. Use environment variables for real credentials"
    echo "3. Run this script again to verify"
    echo ""
    exit 1
else
    echo "✅ No exposed credentials detected - safe to commit!"
    exit 0
fi
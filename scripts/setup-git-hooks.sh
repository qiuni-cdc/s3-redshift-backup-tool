#!/bin/bash
# Setup Git Hooks for Credential Protection
# Run this script once to set up all security measures

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🔧 Setting up Git security hooks...${NC}"

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo -e "${RED}❌ Error: Not in a git repository${NC}"
    exit 1
fi

# 1. Install pre-commit if not already installed
echo -e "${YELLOW}📦 Installing pre-commit...${NC}"
if command -v pre-commit &> /dev/null; then
    echo -e "${GREEN}✅ pre-commit already installed${NC}"
else
    pip install pre-commit
    echo -e "${GREEN}✅ pre-commit installed${NC}"
fi

# 2. Install pre-commit hooks
echo -e "${YELLOW}🪝 Installing pre-commit hooks...${NC}"
pre-commit install
pre-commit install --hook-type commit-msg
echo -e "${GREEN}✅ Pre-commit hooks installed${NC}"

# 3. Install GitLeaks if available
echo -e "${YELLOW}🔍 Checking for GitLeaks...${NC}"
if command -v gitleaks &> /dev/null; then
    echo -e "${GREEN}✅ GitLeaks already installed${NC}"
else
    echo -e "${YELLOW}⚠️  GitLeaks not found. Install with:${NC}"
    echo -e "   ${BLUE}brew install gitleaks${NC} (macOS)"
    echo -e "   ${BLUE}go install github.com/zricethezav/gitleaks/v8@latest${NC} (Go)"
fi

# 4. Install detect-secrets
echo -e "${YELLOW}🕵️  Installing detect-secrets...${NC}"
pip install detect-secrets
echo -e "${GREEN}✅ detect-secrets installed${NC}"

# 5. Create secrets baseline
echo -e "${YELLOW}📋 Creating secrets baseline...${NC}"
if [ ! -f ".secrets.baseline" ]; then
    detect-secrets scan --baseline .secrets.baseline
    echo -e "${GREEN}✅ Secrets baseline created${NC}"
else
    echo -e "${GREEN}✅ Secrets baseline already exists${NC}"
fi

# 6. Create pre-push hook
echo -e "${YELLOW}🚀 Setting up pre-push hook...${NC}"
cat > .git/hooks/pre-push << 'EOF'
#!/bin/bash
# Pre-push hook - Final credential check before pushing to remote

echo "🔍 Running final security check before push..."

# Run our custom credential checker
find . -type f \( -name "*.py" -o -name "*.sql" -o -name "*.md" -o -name "*.json" -o -name "*.yaml" -o -name "*.yml" -o -name "*.toml" \) \
  -not -path "./.git/*" \
  -not -path "./test_env/*" \
  -not -path "./__pycache__/*" \
  | xargs ./scripts/check-credentials.sh

if [ $? -ne 0 ]; then
    echo "❌ Push cancelled - credential exposure prevented"
    exit 1
fi

echo "✅ Security check passed - safe to push"
exit 0
EOF

chmod +x .git/hooks/pre-push
echo -e "${GREEN}✅ Pre-push hook installed${NC}"

# 7. Test the setup
echo -e "${YELLOW}🧪 Testing security setup...${NC}"
pre-commit run --all-files || true
echo -e "${GREEN}✅ Security setup test completed${NC}"

# 8. Final summary
echo ""
echo -e "${GREEN}🎉 Git security setup complete!${NC}"
echo ""
echo -e "${BLUE}Security measures now active:${NC}"
echo -e "  ✅ Pre-commit hooks (run on every commit)"
echo -e "  ✅ Pre-push hooks (run before every push)" 
echo -e "  ✅ GitLeaks integration (if installed)"
echo -e "  ✅ detect-secrets scanning"
echo -e "  ✅ Custom credential checker"
echo -e "  ✅ GitHub Actions security scan (on push)"
echo ""
echo -e "${YELLOW}📋 To manually run security checks:${NC}"
echo -e "  ${BLUE}pre-commit run --all-files${NC}          # Run all checks"
echo -e "  ${BLUE}./scripts/check-credentials.sh *.py${NC}  # Check specific files"
echo -e "  ${BLUE}gitleaks detect${NC}                      # Run GitLeaks"
echo ""
echo -e "${GREEN}Your repository is now protected against credential exposure!${NC}"
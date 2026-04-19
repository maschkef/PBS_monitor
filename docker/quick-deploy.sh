#!/bin/bash
# PBS Monitor Docker Quickstart Deployment
set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BOLD}🐳 PBS Monitor Docker Quickstart${NC}"
echo "Setting up PBS Monitor with Docker..."

# Check if docker and docker-compose are available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null 2>&1; then
    echo -e "${RED}❌ Docker Compose is not available. Please install Docker Compose.${NC}"
    exit 1
fi

# Determine docker-compose command
if docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

# Create project directory
PROJECT_DIR="pbs-monitor"
if [ -d "$PROJECT_DIR" ]; then
    echo -e "${YELLOW}⚠️  Directory $PROJECT_DIR already exists.${NC}"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

# Download configuration files
echo "📥 Downloading configuration files..."

# Download docker-compose.yml
if ! curl -sLf -o docker-compose.yml "https://github.com/maschkef/PBS_monitor/releases/latest/download/docker-compose.yml"; then
    echo -e "${RED}❌ Failed to download docker-compose.yml${NC}"
    exit 1
fi

# Download .env template
if ! curl -sLf -o .env.example "https://raw.githubusercontent.com/maschkef/PBS_monitor/main/.env.example"; then
    echo -e "${RED}❌ Failed to download .env.example${NC}"
    exit 1
fi

# Create .env from template if it doesn't exist
if [ ! -f .env ]; then
    cp .env.example .env
    echo -e "${YELLOW}📝 Created .env from template${NC}"
fi

# Create config directory
mkdir -p config

# Configuration prompt
echo
echo -e "${BOLD}🔧 Configuration${NC}"
echo "Please configure your API settings in .env file:"
echo
echo -e "${YELLOW}Required: API_KEY from https://dashboard.remote-backups.com/settings/security${NC}"
echo

# Check if API_KEY is set
if ! grep -q "^API_KEY=" .env || grep -q "^API_KEY=$" .env; then
    echo -e "${YELLOW}⚠️  API_KEY is not configured in .env${NC}"
    read -p "Enter your API_KEY now (or press Enter to edit .env manually): " api_key
    
    if [ ! -z "$api_key" ]; then
        # Escape characters that are special in sed replacement strings (/, &, \)
        api_key_escaped=$(printf '%s' "$api_key" | sed 's/[\/&]/\\&/g')
        sed -i "s/^API_KEY=.*/API_KEY=$api_key_escaped/" .env
        echo -e "${GREEN}✅ API_KEY configured${NC}"
    else
        echo -e "${YELLOW}Please edit .env file and set your API_KEY, then run:${NC}"
        echo "  $COMPOSE_CMD up -d"
        exit 0
    fi
fi

# Start services
echo
echo -e "${BOLD}🚀 Starting PBS Monitor services...${NC}"
$COMPOSE_CMD pull
$COMPOSE_CMD up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 5

# Check if services are running
if $COMPOSE_CMD ps | grep -q "Up"; then
    echo
    echo -e "${GREEN}✅ PBS Monitor is running!${NC}"
    echo
    echo -e "${BOLD}📱 Access the Web UI:${NC}"
    echo "  http://localhost:5111"
    echo
    echo -e "${BOLD}🔧 Manage services:${NC}"
    echo "  View logs:    $COMPOSE_CMD logs -f"
    echo "  Stop:         $COMPOSE_CMD stop"  
    echo "  Restart:      $COMPOSE_CMD restart"
    echo "  Update:       $COMPOSE_CMD pull && $COMPOSE_CMD up -d"
    echo "  Remove:       $COMPOSE_CMD down"
    echo
    echo -e "${BOLD}📂 Configuration files:${NC}"
    echo "  Environment:  .env"
    echo "  Alerting:     config/config.json (created automatically)"
    echo
else
    echo -e "${RED}❌ Failed to start services${NC}"
    echo "Check logs with: $COMPOSE_CMD logs"
    exit 1
fi
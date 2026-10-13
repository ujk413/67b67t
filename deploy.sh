#!/bin/bash
# Complete Server Deployment Script for 67chat.ru
# Run this on the server: bash deploy.sh

set -e

echo "🚀 Starting deployment for 67chat.ru..."
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DOMAIN="67chat.ru"
PROJECT_DIR="/root/67b67tmess"
REPO_URL="https://github.com/yourusername/67b67tmess.git"  # Update this if using git

echo -e "${GREEN}Step 1: Checking system requirements...${NC}"
# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    systemctl start docker
    systemctl enable docker
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Installing Docker Compose..."
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
fi

echo -e "${GREEN}✓ Docker and Docker Compose installed${NC}"
echo ""

echo -e "${GREEN}Step 2: Setting up project directory...${NC}"
# Create project directory if it doesn't exist
if [ ! -d "$PROJECT_DIR" ]; then
    echo "Project directory not found. Please upload your project files to $PROJECT_DIR"
    echo "You can use: scp -r ./67b67tmess root@<SERVER_IP>:/root/"
    exit 1
fi

cd "$PROJECT_DIR"
echo -e "${GREEN}✓ Project directory: $PROJECT_DIR${NC}"
echo ""

echo -e "${GREEN}Step 3: Generating encryption key...${NC}"
# Generate ENCRYPTION_KEY if not exists
if ! grep -q "ENCRYPTION_KEY=" .env 2>/dev/null; then
    ENCRYPTION_KEY=$(openssl rand -base64 32)
    echo "ENCRYPTION_KEY=$ENCRYPTION_KEY" >> .env
    echo -e "${GREEN}✓ Generated ENCRYPTION_KEY${NC}"
else
    echo -e "${YELLOW}⚠ ENCRYPTION_KEY already exists${NC}"
fi
echo ""

echo -e "${GREEN}Step 4: Configuring environment...${NC}"
# Update .env with domain
if [ -f .env ]; then
    sed -i "s|ORIGIN=.*|ORIGIN=https://$DOMAIN|g" .env
    sed -i "s|TURN_REALM=.*|TURN_REALM=$DOMAIN|g" .env
    echo -e "${GREEN}✓ Updated .env with domain $DOMAIN${NC}"
else
    echo -e "${RED}✗ .env file not found!${NC}"
    exit 1
fi
echo ""

echo -e "${GREEN}Step 5: Running database migrations...${NC}"
# Start PostgreSQL first
docker-compose up -d postgres
sleep 10

# Run migrations
for migration in db/migrations/*.sql; do
    if [ -f "$migration" ]; then
        echo "Running migration: $(basename $migration)"
        docker-compose exec -T postgres psql -U messenger -d messenger < "$migration" || echo "Migration may have already been applied"
    fi
done
echo -e "${GREEN}✓ Database migrations completed${NC}"
echo ""

echo -e "${GREEN}Step 6: Starting all services...${NC}"
# Start all services
docker-compose up -d

# Wait for services to start
echo "Waiting for services to start..."
sleep 15

echo -e "${GREEN}✓ All services started${NC}"
echo ""

echo -e "${GREEN}Step 7: Checking Tor Hidden Service...${NC}"
# Get Tor onion address
# The tor container mounts ./data/tor (see docker-compose.yml)
if [ -s "data/tor/hostname" ]; then
    ONION_ADDRESS=$(tr -d '
' < data/tor/hostname)
    echo -e "${GREEN}✓ Tor Hidden Service: http://$ONION_ADDRESS${NC}"
    echo "  (add it to ORIGIN in .env, comma-separated, to allow CORS; show again: ./scripts/show_onion.sh)"
else
    echo -e "${YELLOW}⚠ Tor hostname not found yet. Wait a few seconds, then run ./scripts/show_onion.sh or check: docker-compose logs tor${NC}"
fi
echo ""

echo -e "${GREEN}Step 8: Checking service status...${NC}"
docker-compose ps
echo ""

echo -e "${GREEN}Step 9: Testing API...${NC}"
# Test API health
if curl -f http://localhost/api/health &> /dev/null; then
    echo -e "${GREEN}✓ API is responding${NC}"
else
    echo -e "${RED}✗ API is not responding${NC}"
    echo "Check logs: docker-compose logs api"
fi
echo ""

echo -e "${GREEN}Step 10: SSL Certificate (Caddy will auto-generate)...${NC}"
echo "Caddy will automatically obtain SSL certificate for $DOMAIN"
echo "Make sure DNS A record points to this server IP: $(curl -s ifconfig.me)"
echo ""

echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}🎉 Deployment Complete!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
echo ""
echo "📱 Web App: https://$DOMAIN"
echo "🔐 Tor: http://$ONION_ADDRESS (if available)"
echo "🔧 API: https://$DOMAIN/api"
echo ""
echo "📊 Useful commands:"
echo "  - View logs: docker-compose logs -f"
echo "  - Restart: docker-compose restart"
echo "  - Stop: docker-compose down"
echo "  - Update: git pull && docker-compose up -d --build"
echo ""
echo "🔑 Next steps:"
echo "  1. Test web app: https://$DOMAIN"
echo "  2. Create test account"
echo "  3. Test E2E encryption"
echo "  4. Check Tor address: ./scripts/show_onion.sh"
echo ""
echo -e "${YELLOW}⚠ Important:${NC}"
echo "  - Save your ENCRYPTION_KEY from .env file"
echo "  - Set up backups for database"
echo "  - Monitor logs regularly"
echo ""

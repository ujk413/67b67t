# Server Deployment Guide for 67b67t.ru

## Quick Deploy

### 1. Upload Project to Server

```bash
# From your local machine
scp -r C:\67b67tmess root@<SERVER_IP>:/root/

# Or use WinSCP, FileZilla, or any SFTP client
```

### 2. Connect to Server

```bash
ssh root@<SERVER_IP>
# Password: <SERVER_PASSWORD>
```

### 3. Run Deployment Script

```bash
cd /root/67b67tmess
chmod +x deploy.sh
./deploy.sh
```

## Manual Deployment Steps

If you prefer to deploy manually:

### 1. Install Docker

```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
systemctl start docker
systemctl enable docker
```

### 2. Install Docker Compose

```bash
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
```

### 3. Setup Project

```bash
cd /root/67b67tmess

# Generate encryption key
ENCRYPTION_KEY=$(openssl rand -base64 32)
echo "ENCRYPTION_KEY=$ENCRYPTION_KEY" >> .env

# Update domain in .env
sed -i 's|ORIGIN=.*|ORIGIN=https://67b67t.ru|g' .env
sed -i 's|TURN_REALM=.*|TURN_REALM=67b67t.ru|g' .env
```

### 4. Run Database Migrations

```bash
# Start PostgreSQL
docker-compose up -d postgres
sleep 10

# Run migrations
docker-compose exec -T postgres psql -U messenger -d messenger < db/migrations/001_add_session_tracking.sql
docker-compose exec -T postgres psql -U messenger -d messenger < db/migrations/002_add_unread_tracking.sql
docker-compose exec -T postgres psql -U messenger -d messenger < db/migrations/003_add_fcm_token.sql
```

### 5. Start All Services

```bash
docker-compose up -d
```

### 6. Check Status

```bash
# View all services
docker-compose ps

# View logs
docker-compose logs -f

# Check API health
curl http://localhost/api/health
```

### 7. Get Tor Address

```bash
# Wait a few seconds for Tor to generate address
cat tor/hidden_service/hostname
```

## DNS Configuration

Point your domain to the server:

```
A record: 67b67t.ru → <SERVER_IP>
```

Caddy will automatically obtain SSL certificate.

## Firewall Configuration

```bash
# Allow HTTP/HTTPS
ufw allow 80/tcp
ufw allow 443/tcp

# Allow SSH
ufw allow 22/tcp

# Enable firewall
ufw enable
```

## Testing

### 1. Test Web App

```bash
curl https://67b67t.ru
```

### 2. Test API

```bash
curl https://67b67t.ru/api/health
```

### 3. Test Tor

```bash
# Get onion address
cat tor/hidden_service/hostname

# Test (requires Tor Browser or torsocks)
torsocks curl http://your-onion-address.onion
```

## Monitoring

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f api
docker-compose logs -f postgres
docker-compose logs -f tor
docker-compose logs -f caddy
```

### Check Resource Usage

```bash
docker stats
```

## Maintenance

### Update Application

```bash
cd /root/67b67tmess
git pull  # if using git
docker-compose up -d --build
```

### Restart Services

```bash
docker-compose restart
```

### Stop Services

```bash
docker-compose down
```

### Backup Database

```bash
docker-compose exec postgres pg_dump -U messenger messenger > backup_$(date +%Y%m%d).sql
```

### Restore Database

```bash
docker-compose exec -T postgres psql -U messenger messenger < backup_20260410.sql
```

## Troubleshooting

### Services Not Starting

```bash
# Check logs
docker-compose logs

# Check disk space
df -h

# Check memory
free -h
```

### SSL Certificate Issues

```bash
# Check Caddy logs
docker-compose logs caddy

# Verify DNS
nslookup 67b67t.ru

# Test port 80/443
curl -I http://67b67t.ru
```

### Database Connection Issues

```bash
# Check PostgreSQL logs
docker-compose logs postgres

# Test connection
docker-compose exec postgres psql -U messenger -d messenger -c "SELECT 1;"
```

### Tor Not Working

```bash
# Check Tor logs
docker-compose logs tor

# Verify hostname file
ls -la tor/hidden_service/

# Restart Tor
docker-compose restart tor
```

## Security Checklist

- [ ] ENCRYPTION_KEY generated and saved
- [ ] Strong database password set
- [ ] Firewall configured (ufw)
- [ ] SSL certificate obtained (Caddy auto)
- [ ] Regular backups configured
- [ ] Monitoring set up
- [ ] Logs rotation configured

## Performance Tuning

### PostgreSQL

Edit `docker-compose.yml`:

```yaml
postgres:
  environment:
    - POSTGRES_SHARED_BUFFERS=256MB
    - POSTGRES_EFFECTIVE_CACHE_SIZE=1GB
    - POSTGRES_MAX_CONNECTIONS=100
```

### API

Edit `docker-compose.yml`:

```yaml
api:
  environment:
    - NODE_ENV=production
    - MAX_WORKERS=4
```

## Useful Commands

```bash
# View all containers
docker ps -a

# View images
docker images

# Clean up unused images
docker image prune -a

# Clean up unused volumes
docker volume prune

# View disk usage
docker system df

# Complete cleanup
docker system prune -a --volumes
```

## Support

- Documentation: See README.md files
- Logs: `docker-compose logs -f`
- Health check: `curl http://localhost/api/health`

---

**Server:** <SERVER_IP>  
**Domain:** 67b67t.ru  
**Project:** /root/67b67tmess  

**Ready to deploy!** 🚀

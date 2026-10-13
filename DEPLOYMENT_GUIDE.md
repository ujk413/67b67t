# Complete Deployment Instructions

## Overview

This guide will help you:
1. Upload project to server
2. Deploy web application
3. Build Android APK
4. Configure domain and SSL
5. Setup Tor Hidden Service

**Server:** <SERVER_IP>  
**Domain:** 67b67t.ru  
**User:** root  
**Password:** <SERVER_PASSWORD>

---

## Option 1: Automated Deployment (Recommended)

### Windows

```cmd
cd C:\67b67tmess
upload_and_deploy.cmd
```

This will:
- Upload project to server
- Show next steps

### Linux/Mac

```bash
cd /path/to/67b67tmess
./upload_and_deploy.sh
```

---

## Option 2: Manual Deployment

### Step 1: Upload Project

**Using WinSCP (Windows):**
1. Download WinSCP: https://winscp.net/
2. Connect to: <SERVER_IP>
3. Username: root
4. Password: <SERVER_PASSWORD>
5. Upload entire `67b67tmess` folder to `/root/`

**Using SCP (Linux/Mac):**
```bash
scp -r ./67b67tmess root@<SERVER_IP>:/root/
```

**Using FileZilla:**
1. Download FileZilla: https://filezilla-project.org/
2. Host: sftp://<SERVER_IP>
3. Username: root
4. Password: <SERVER_PASSWORD>
5. Port: 22
6. Upload `67b67tmess` to `/root/`

### Step 2: Connect to Server

**Windows (PuTTY):**
1. Download PuTTY: https://www.putty.org/
2. Host: <SERVER_IP>
3. Port: 22
4. Username: root
5. Password: <SERVER_PASSWORD>

**Linux/Mac:**
```bash
ssh root@<SERVER_IP>
# Password: <SERVER_PASSWORD>
```

### Step 3: Deploy Web Application

```bash
cd /root/67b67tmess
chmod +x deploy.sh
./deploy.sh
```

This will:
- Install Docker & Docker Compose
- Generate encryption key
- Configure domain (67b67t.ru)
- Run database migrations
- Start all services (API, PostgreSQL, Caddy, Tor)
- Setup SSL certificate (automatic via Caddy)
- Create Tor Hidden Service

### Step 4: Verify Deployment

```bash
# Check services
docker-compose ps

# Check logs
docker-compose logs -f

# Test API
curl http://localhost/api/health

# Get Tor address
cat tor/hidden_service/hostname
```

### Step 5: Build Android APK (Optional)

```bash
cd /root/67b67tmess
chmod +x build_apk.sh
./build_apk.sh
```

This will:
- Install Node.js & Android SDK
- Build APK
- Save to `/root/67b67tmess/builds/messenger67-latest.apk`

**Download APK:**
```bash
# From your local machine
scp root@<SERVER_IP>:/root/67b67tmess/builds/messenger67-latest.apk .
```

---

## DNS Configuration

**IMPORTANT:** Point your domain to the server:

1. Go to your DNS provider (e.g., Cloudflare, GoDaddy)
2. Add A record:
   ```
   Type: A
   Name: @
   Value: <SERVER_IP>
   TTL: Auto
   ```
3. Wait 5-10 minutes for DNS propagation
4. Verify: `nslookup 67b67t.ru`

Caddy will automatically obtain SSL certificate once DNS is configured.

---

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

---

## Post-Deployment Checklist

### Web Application
- [ ] Services running: `docker-compose ps`
- [ ] API responding: `curl https://67b67t.ru/api/health`
- [ ] Web app accessible: https://67b67t.ru
- [ ] SSL certificate obtained (check browser)
- [ ] Can register account
- [ ] Can send messages
- [ ] E2E encryption working

### Tor Hidden Service
- [ ] Tor running: `docker-compose logs tor`
- [ ] Hostname generated: `cat tor/hidden_service/hostname`
- [ ] Accessible via Tor Browser

### Database
- [ ] Migrations applied
- [ ] Can create users
- [ ] Can create rooms
- [ ] Data persists after restart

### Mobile App (if built)
- [ ] APK downloaded
- [ ] APK installs on device
- [ ] Can connect to server
- [ ] Can login/register
- [ ] Push notifications work (after Firebase setup)

---

## Accessing Your Messenger

### Web App
- **HTTPS:** https://67b67t.ru
- **HTTP:** http://67b67t.ru (redirects to HTTPS)

### Tor Hidden Service
```bash
# Get onion address
ssh root@<SERVER_IP> "cat /root/67b67tmess/tor/hidden_service/hostname"

# Access via Tor Browser
http://your-onion-address.onion
```

### Mobile App
1. Download APK from server
2. Install on Android device
3. Open app
4. Enter server URL: https://67b67t.ru/api
5. Register/Login

---

## Monitoring

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f api
docker-compose logs -f postgres
docker-compose logs -f caddy
docker-compose logs -f tor
```

### Check Status
```bash
# Service status
docker-compose ps

# Resource usage
docker stats

# Disk space
df -h

# Memory
free -h
```

---

## Maintenance

### Update Application
```bash
cd /root/67b67tmess
# Upload new files via SCP/WinSCP
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

# Download backup
scp root@<SERVER_IP>:/root/67b67tmess/backup_*.sql .
```

### Restore Database
```bash
docker-compose exec -T postgres psql -U messenger messenger < backup_20260410.sql
```

---

## Troubleshooting

### Services Not Starting
```bash
# Check logs
docker-compose logs

# Check disk space
df -h

# Restart services
docker-compose restart
```

### SSL Certificate Not Working
```bash
# Check Caddy logs
docker-compose logs caddy

# Verify DNS
nslookup 67b67t.ru

# Should point to: <SERVER_IP>
```

### Can't Access Web App
```bash
# Check if services are running
docker-compose ps

# Check firewall
ufw status

# Test locally
curl http://localhost/api/health
```

### Tor Not Working
```bash
# Check Tor logs
docker-compose logs tor

# Restart Tor
docker-compose restart tor

# Check hostname
cat tor/hidden_service/hostname
```

---

## Security Recommendations

1. **Change root password:**
   ```bash
   passwd
   ```

2. **Create non-root user:**
   ```bash
   adduser deploy
   usermod -aG sudo deploy
   ```

3. **Setup SSH key authentication:**
   ```bash
   ssh-keygen -t ed25519
   ssh-copy-id root@<SERVER_IP>
   ```

4. **Disable password authentication:**
   ```bash
   nano /etc/ssh/sshd_config
   # Set: PasswordAuthentication no
   systemctl restart sshd
   ```

5. **Setup fail2ban:**
   ```bash
   apt-get install fail2ban
   systemctl enable fail2ban
   ```

6. **Regular backups:**
   ```bash
   # Add to crontab
   0 2 * * * cd /root/67b67tmess && docker-compose exec postgres pg_dump -U messenger messenger > backup_$(date +\%Y\%m\%d).sql
   ```

---

## Support

- **Documentation:** See README.md and other .md files
- **Logs:** `docker-compose logs -f`
- **Health Check:** `curl http://localhost/api/health`
- **Server Deploy Guide:** SERVER_DEPLOY.md

---

## Quick Reference

```bash
# Connect to server
ssh root@<SERVER_IP>

# Deploy
cd /root/67b67tmess && ./deploy.sh

# Check status
docker-compose ps

# View logs
docker-compose logs -f

# Restart
docker-compose restart

# Get Tor address
cat tor/hidden_service/hostname

# Build APK
./build_apk.sh

# Download APK
scp root@<SERVER_IP>:/root/67b67tmess/builds/messenger67-latest.apk .
```

---

**Ready to deploy!** 🚀

Follow the steps above to get your messenger running on 67b67t.ru

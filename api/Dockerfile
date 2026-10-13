FROM node:20-alpine

# ffmpeg powers voice-message / video-note transcoding and thumbnails.
RUN apk add --no-cache ffmpeg wget

WORKDIR /app

# Install dependencies from the lockfile only. `npm ci` is reproducible and
# fails loudly if package.json and the lockfile drift; `npm install` at runtime
# (the previous setup) silently resolved fresh, unpinned versions on every boot.
# Copy only the manifests first so this layer stays cached until dependencies
# change (source edits no longer trigger a reinstall).
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

# Application source (node_modules/backups are excluded by .dockerignore).
COPY . .

# Drop privileges: the app only needs to read its code and write ./uploads.
# Run as the bundled non-root `node` user (uid 1000).
RUN mkdir -p /app/uploads && chown -R node:node /app
USER node

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -qO- http://127.0.0.1:3000/health || exit 1

CMD ["node", "server.js"]

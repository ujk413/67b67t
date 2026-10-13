// Local dev proxy that mirrors the Caddy routing in Caddyfile:
//   /api/*  -> API (prefix stripped)
//   /ws     -> API WebSocket (raw upgrade pipe)
//   others  -> web/ static files (index.html fallback)
// Usage: node scripts/dev_proxy.mjs [port=8080] [apiPort=3999]
import http from 'node:http';
import net from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'web');
const port = Number(process.env.PORT || process.argv[2] || 8080);
const apiPort = Number(process.argv[3] || 3999);
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.webmanifest': 'application/manifest+json', '.png': 'image/png', '.svg': 'image/svg+xml' };

const server = http.createServer((req, res) => {
  if (req.url.startsWith('/api/') || req.url === '/api') {
    const upstream = http.request({
      host: '127.0.0.1', port: apiPort, method: req.method,
      path: req.url.replace(/^\/api/, '') || '/',
      headers: { ...req.headers, host: `127.0.0.1:${apiPort}` }
    }, (up) => {
      res.writeHead(up.statusCode, up.headers);
      up.pipe(res);
    });
    upstream.on('error', () => { res.writeHead(502); res.end('upstream down'); });
    req.pipe(upstream);
    return;
  }
  const urlPath = decodeURIComponent(req.url.split('?')[0]);
  let file = path.join(root, urlPath === '/' ? 'index.html' : urlPath);
  if (!file.startsWith(root) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) file = path.join(root, 'index.html');
  res.writeHead(200, { 'content-type': TYPES[path.extname(file)] || 'application/octet-stream', 'cache-control': 'no-store' });
  fs.createReadStream(file).pipe(res);
});

server.on('upgrade', (req, socket, head) => {
  if (!req.url.startsWith('/ws')) { socket.destroy(); return; }
  const up = net.connect(apiPort, '127.0.0.1', () => {
    const headers = Object.entries({ ...req.headers, host: `127.0.0.1:${apiPort}` }).map(([k, v]) => `${k}: ${v}`).join('\r\n');
    up.write(`${req.method} ${req.url} HTTP/1.1\r\n${headers}\r\n\r\n`);
    if (head && head.length) up.write(head);
    socket.pipe(up).pipe(socket);
  });
  up.on('error', () => socket.destroy());
  socket.on('error', () => up.destroy());
});

server.listen(port, '127.0.0.1', () => console.log(`dev proxy on http://127.0.0.1:${port} -> api :${apiPort}`));

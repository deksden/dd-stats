import { createReadStream, existsSync, statSync } from 'node:fs';
import { extname, join, normalize } from 'node:path';
import http from 'node:http';

const PORT = Number(process.env.PORT || 4173);
const ROOT = join(process.cwd(), 'dist');

const MIME_TYPES = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml; charset=utf-8',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8',
};

function resolvePath(urlPathname) {
  const pathname = decodeURIComponent(urlPathname.split('?')[0]);
  const safePath = normalize(pathname).replace(/^(\.\.[/\\])+/, '');
  const target = safePath === '/' ? '/index.html' : safePath;
  return join(ROOT, target);
}

function sendFile(res, filePath) {
  const extension = extname(filePath).toLowerCase();
  const contentType = MIME_TYPES[extension] || 'application/octet-stream';
  res.writeHead(200, { 'Content-Type': contentType });
  createReadStream(filePath).pipe(res);
}

const server = http.createServer((req, res) => {
  try {
    let filePath = resolvePath(req.url || '/');

    if (!existsSync(filePath)) {
      filePath = join(ROOT, 'index.html');
    } else if (statSync(filePath).isDirectory()) {
      filePath = join(filePath, 'index.html');
    }

    if (!existsSync(filePath)) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }

    sendFile(res, filePath);
  } catch (error) {
    res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(`Server error: ${error.message}`);
  }
});

server.listen(PORT, () => {
  console.log(`dd-stats dashboard available at http://localhost:${PORT}`);
});

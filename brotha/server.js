/* Switchboard — a tiny LAN relay (~70 lines).
   Run on ONE machine:   npm install ws     then     node server.js
   Every other machine opens:  http://<this-machine-ip>:3000  */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const BOOT_ID = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
let seq = 0;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url.startsWith('/index')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  } else { res.writeHead(404); res.end('not found'); }
});

const wss = new WebSocketServer({ server });
const brief  = ws => ({ id: ws.id, name: ws.name, ip: ws.ip });
const roster = () => [...wss.clients].filter(c => c.readyState === 1).map(brief);
const toOne  = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const toAll  = o => { const s = JSON.stringify(o); for (const c of wss.clients) if (c.readyState === 1) c.send(s); };
const clean  = s => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 24);

wss.on('connection', (ws, req) => {
  ws.id = 'n' + (++seq);
  ws.ip = (req.socket.remoteAddress || '').replace(/^::ffff:/, '');
  if (ws.ip === '::1') ws.ip = '127.0.0.1';
  ws.name = 'NODE-' + String(seq).padStart(2, '0');
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  toOne(ws, { type: 'welcome', id: ws.id, ip: ws.ip, name: ws.name, boot: BOOT_ID, peers: roster() });
  toAll({ type: 'peers', peers: roster() });

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'hello' || m.type === 'rename') {
      const name = clean(m.name);
      if (name) ws.name = name;
      toAll({ type: 'peers', peers: roster() });

    } else if (m.type === 'msg') {
      // route ONLY to the addressed machine — that's the whole point
      const target = [...wss.clients].find(c => c.id === m.to);
      if (target && target.readyState === 1) {
        toOne(target, { type: 'msg', from: ws.id, name: ws.name, ip: ws.ip, text: String(m.text).slice(0, 2000), ts: m.ts || Date.now() });
        toOne(ws, { type: 'ack', to: m.to, ts: m.ts });          // delivery confirmed
      } else {
        toOne(ws, { type: 'fail', to: m.to, ts: m.ts });          // host unreachable
      }

    } else if (m.type === 'typing') {
      const target = [...wss.clients].find(c => c.id === m.to);
      if (target && target.readyState === 1) toOne(target, { type: 'typing', from: ws.id });

    } else if (m.type === 'ping') {
      toOne(ws, { type: 'pong', t: m.t });
    }
  });

  ws.on('close', () => toAll({ type: 'peers', peers: roster() }));
  ws.on('error', () => {});
});

// prune dead sockets every 30s
setInterval(() => {
  for (const c of wss.clients) {
    if (c.isAlive === false) { c.terminate(); continue; }
    c.isAlive = false; c.ping();
  }
}, 30000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n  Switchboard relay listening');
  console.log(`  On this machine : http://localhost:${PORT}`);
  console.log(`  On the LAN      : http://<this-machine-ip>:${PORT}\n`);
});

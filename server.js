const express = require('express');
const { HorseRacingAPI } = require('hkjc-api');
const mqtt = require('mqtt');
const zlib = require('zlib');

const app = express();
app.use(express.json({ limit: '2mb' }));

// ── [修改 1] PORT 改為讀取環境變數，Railway 會自動注入 PORT ──────────────────
const PORT = process.env.PORT || 3000;

// ── [修改 2] CORS middleware — 允許 python-service 跨域呼叫 /push ──────────
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── SSE 客戶端池 ──────────────────────────────────────────────────────────────
const sseClients = new Set();

app.get('/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  // ── [修改 3] 禁止 Railway / Nginx proxy 緩衝 SSE ──────────────────────────
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();
  sseClients.add(res);
  const hb = setInterval(() => res.write(': hb\n\n'), 15000);
  req.on('close', () => { clearInterval(hb); sseClients.delete(res); });
});

function broadcastSSE(payload) {
  const str = `data: ${JSON.stringify(payload)}\n\n`;
  sseClients.forEach(c => { try { c.write(str); } catch(e) {} });
}

// odd.py 呼叫此 endpoint 推送數據
app.post('/push', (req, res) => {
  broadcastSSE(req.body);
  res.json({ ok: true, clients: sseClients.size });
});

// ── [修改 4] Health check endpoint — Railway 用來確認服務存活 ────────────────
app.get('/health', (req, res) => {
  res.json({ ok: true, uptime: process.uptime(), mqttClients: sseClients.size });
});

// ── MQTT 實時緩存 ─────────────────────────────────────────────────────────────
const mqttLive = {};

function getBigLabel(v) {
  const n = Number(v || 0);
  if (n >= 100000) return '

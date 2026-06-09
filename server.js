const express = require('express');
const { HorseRacingAPI } = require('hkjc-api');
const mqtt = require('mqtt');
const zlib = require('zlib');

const app = express();
app.use(express.json({ limit: '2mb' }));

const PORT = process.env.PORT || 3000;

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const sseClients = new Set();

app.get('/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();
  sseClients.add(res);
  const hb = setInterval(() => res.write(': hb

'), 15000);
  req.on('close', () => { clearInterval(hb); sseClients.delete(res); });
});

function broadcastSSE(payload) {
  const str = 'data: ' + JSON.stringify(payload) + '

';
  sseClients.forEach(c => { try { c.write(str); } catch(e) {} });
}

app.post('/push', (req, res) => {
  broadcastSSE(req.body);
  res.json({ ok: true, clients: sseClients.size });
});

app.get('/health', (req, res) => {
  res.json({ ok: true, uptime: process.uptime(), sseClients: sseClients.size });
});

const mqttLive = {};

function getBigLabel(v) {
  const n = Number(v || 0);
  if (n >= 100000) return '>=100k';
  if (n > 10000) return '10k~100k';
  if (n > 0) return '<=10k';
  return '';
}

function getToday() {
  const d = new Date();
  return String(d.getFullYear()) +
    String(d.getMonth() + 1).padStart(2, '0') +
    String(d.getDate()).padStart(2, '0');
}

function initMqtt() {
  const today = getToday();
  const client = mqtt.connect('wss://ueb.hkjc.com:52443/', {
    username: 'jcbw2',
    password: '2Wt5tGOzRm]yp~N',
    clientId: 'jcbw2_' + Date.now(

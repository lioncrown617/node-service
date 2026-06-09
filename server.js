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
  const HB = ': hb' + String.fromCharCode(10) + String.fromCharCode(10);
  const hb = setInterval(() => res.write(HB), 15000);
  req.on('close', () => { clearInterval(hb); sseClients.delete(res); });
});

function broadcastSSE(payload) {
  const NL = String.fromCharCode(10);
  const str = 'data: ' + JSON.stringify(payload) + NL + NL;
  sseClients.forEach(function(c) { try { c.write(str); } catch(e) {} });
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
    clientId: 'jcbw2_' + Date.now(),
    protocolVersion: 5,
    rejectUnauthorized: false,
    reconnectPeriod: 3000,
    connectTimeout: 10000,
    keepalive: 30,
  });

  client.on('connect', () => {
    console.log('[MQTT] Connected!');
    const base = 'hk/d/prdt/wager/evt/01/upd/racing/' + today;
    client.subscribe(base + '/+/+/win/+/expr/odds/full');
    client.subscribe(base + '/+/+/pla/+/expr/odds/full');
    client.subscribe(base + '/+/+/qin/+/expr/odds/full');
    client.subscribe(base + '/+/+/qpl/+/expr/odds/full');
    client.subscribe(base + '/+/+/win/inv');
    client.subscribe(base + '/+/+/qin/inv');
    client.subscribe(base + '/+/+/qpl/inv');
  });

  client.on('message', (topic, payload) => {
    try {
      const raw = (payload[0] === 0x1f && payload[1] === 0x8b)
        ? zlib.gunzipSync(payload) : payload;
      const data = JSON.parse(raw.toString());
      const parts = topic.split('/');
      const venue = parts[9].toUpperCase();
      const raceNo = String(parseInt(parts[10]));
      const oddsType = parts[11];
      if (!mqttLive[venue]) mqttLive[venue] = {};
      if (!mqttLive[venue][raceNo]) mqttLive[venue][raceNo] = {
        win: {}, pla: {}, qin: {}, qpl: {},
        winInv: {}, qinInv: {}, qplInv: {},
        winBig: {}, qinBig: {}, qplBig: {},
        pool: 0, qinPool: 0, qplPool: 0, updAt: ''
      };
      const slot = mqttLive[venue][raceNo];
      if (data.cbs && ['win','pla','qin','qpl'].includes(oddsType)) {
        const odsMap = {}, invMap = {};
        data.cbs.forEach(function(cb) {
          odsMap[cb.cb] = cb.ods || '';
          invMap[cb.cb] = cb.inv || 0;
        });
        slot[oddsType] = odsMap;
        if (oddsType === 'win') { slot.winInv = invMap; slot.winBig = invMap; }
        if (oddsType === 'qin') { slot.qinInv = invMap; slot.qinBig = invMap; }
        if (oddsType === 'qpl') { slot.qplInv = invMap; slot.qplBig = invMap; }
        slot.pool = data.pInv || slot.pool;
        slot.updAt = data.updAt || '';
      }
      if (topic.endsWith('/win/inv') && data.ttlInv) slot.pool = data.ttlInv.net || slot.pool;
      if (topic.endsWith('/qin/inv') && data.ttlInv) slot.qinPool = data.ttlInv.net || slot.qinPool;
      if (topic.endsWith('/qpl/inv') && data.ttlInv) slot.qplPool = data.ttlInv.net || slot.qplPool;
    } catch(e) {}
  });

  client.on('error', function(e) { console.log('[MQTT ERROR]', e.message); });
  client.on('reconnect', function() { console.log('[MQTT] Reconnecting...'); });

  const now = new Date();
  const midnight = new Date(now);
  midnight.setHours(24, 1, 0, 0);
  setTimeout(function() {
    console.log('[MQTT] New day - reconnecting...');
    client.end(true, function() { initMqtt(); });
  }, midnight - now);
}

initMqtt();

const api = new HorseRacingAPI();
const cardCache = {}, cardCacheTs = {};

async function getCardAndRaceInfo(venue, raceNo) {
  const key = venue + '_' + raceNo;
  const now = Date.now();
  if (cardCache[key] && (now - cardCacheTs[key]) < 300000)
    return { cardMap: cardCache[key], raceInfo: cardCache[key + '_info'] || {} };
  let cardMap = {}, raceInfo = {};
  try {
    const { raceMeetings } = await api.getRaceMeetings();
    const meeting = raceMeetings.find(function(m) { return m.venueCode === venue; });
    if (meeting) {
      const race = meeting.races.find(function(r) { return Number(r.no) === raceNo; });
      if (race) {
        const trackDesc = (race.raceTrack && (race.raceTrack.description_ch || race.raceTrack.description_en)) || '';
        const courseDesc = (race.raceCourse && (race.raceCourse.description_ch || race.raceCourse.description_en)) || '';
        const courseCode = (race.raceCourse && race.raceCourse.displayCode) || '';
        const courseFull = courseDesc ? (courseCode ? courseDesc + '(' + courseCode + ')' : courseDesc) : courseCode;
        let raceTime = race.postTime || '';
        try {
          if (raceTime) {
            const d = new Date(raceTime);
            raceTime = String(d.getHours()).padStart(2,'0') + ':' + String(d.getMinutes()).padStart(2,'0');
          }
        } catch(e) {}
        raceInfo = {
          race_time: raceTime,
          distance: race.distance ? race.distance + 'm' : '',
          track: trackDesc,
          course: courseFull,
          race_class: race.raceClass_ch || race.raceClass_en || race.raceClass || '',
          going: race.go_ch || race.go_en || race.going || '',
          cla_code: race.claCode || '',
          race_name: race.raceName_ch || race.raceName_en || '',
          field_size: race.wageringFieldSize ? String(race.wageringFieldSize) + ' runners' : '',
        };
        cardMap = {};
        for (const r of (race.runners || [])) {
          const no = String(r.no);
          cardMap[no] = {
            name: r.name_ch || r.name_en || '',
            barrier: String(r.barrierDrawNumber || ''),
            jockey: (r.jockey && (r.jockey.name_ch || r.jockey.name_en)) || '',
            trainer: (r.trainer && (r.trainer.name_ch || r.trainer.name_en)) || '',
          };
        }
        cardCache[key] = cardMap;
        cardCache[key + '_info'] = raceInfo;
        cardCacheTs[key] = now;
      }
    }
  } catch(e) {
    console.log('[WARN] getCardAndRaceInfo:', e.message.substring(0, 200));
  }
  return { cardMap, raceInfo };
}

app.get('/odds', async (req, res) => {
  try {
    const { date, venue, raceno } = req.query;
    const raceNo = parseInt(raceno) || 1;
    const raceNoStr = String(raceNo);
    const { cardMap, raceInfo } = await getCardAndRaceInfo(venue, raceNo);
    const live = mqttLive[venue] && mqttLive[venue][raceNoStr];
    if (live && Object.keys(live.win).length > 0) {
      const allNos = Array.from(new Set(Object.keys(live.win).concat(Object.keys(live.pla))))
        .sort(function(a, b) { return Number(a) - Number(b); });
      const results = allNos.map(function(no) {
        const info = cardMap[no] || {};
        return {
          no: no,
          name: info.name || '',
          barrier: info.barrier || '',
          jockey: info.jockey || '',
          trainer: info.trainer || '',
          win: live.win[no] || 'SCR',
          place: live.pla[no] || '',
          win_investment: live.winInv[no] || 0,
          win_big: (live.winBig && live.winBig[no]) || 0,
          win_big_label: getBigLabel((live.winBig && live.winBig[no]) || 0),
          odds_drop_official: '0',
          hot_favourite: false,
        };
      });
      return res.json({
        ok: true,
        results: results,
        win_pool: String(live.pool || ''),
        updAt: live.updAt,
        race_time: raceInfo.race_time || '',
        distance: raceInfo.distance || '',
        track: raceInfo.track || '',
        course: raceInfo.course || '',
        race_class: raceInfo.race_class || '',
        going: raceInfo.going || '',
        cla_code: raceInfo.cla_code || '',
        race_name: raceInfo.race_name || '',
        field_size: raceInfo.field_size || '',
        source: 'mqtt',
      });
    }
    return res.json({ ok: false, error: 'Waiting MQTT data (' + venue + ' R' + raceNo + ')' });
  } catch(e) {
    res.json({ ok: false, error: e.message.substring(0, 300) });
  }
});

app.get('/qin-qpl', async (req, res) => {
  try {
    const { venue, raceno } = req.query;
    const raceNoStr = String(parseInt(raceno) || 1);
    const live = mqttLive[venue] && mqttLive[venue][raceNoStr];
    if (!live || (Object.keys(live.qin || {}).length === 0 && Object.keys(live.qpl || {}).length === 0))
      return res.json({ ok: false, error: 'No QIN/QPL MQTT data yet' });
    const toArr = function(odsMap, invMap) {
      return Object.entries(odsMap || {}).map(function(e) {
        return { combo: e[0], odds: e[1], investment: invMap[e[0]] || 0 };
      });
    };
    res.json({
      ok: true,
      qin: { odds: toArr(live.qin, live.qinInv), pool: String(live.qinPool || ''), count: Object.keys(live.qin || {}).length, big: live.qinBig || {} },
      qpl: { odds: toArr(live.qpl, live.qplInv), pool: String(live.qplPool || ''), count: Object.keys(live.qpl || {}).length, big: live.qplBig || {} },
    });
  } catch(e) {
    res.json({ ok: false, error: e.message.substring(0, 300) });
  }
});

app.get('/mqtt-status', (req, res) => {
  const summary = {};
  for (const v in mqttLive) {
    summary[v] = {};
    for (const rn in mqttLive[v]) {
      const slot = mqttLive[v][rn];
      summary[v][rn] = {
        win: Object.keys(slot.win).length,
        qin: Object.keys(slot.qin).length,
        qpl: Object.keys(slot.qpl).length,
        winBig: Object.keys(slot.winBig || {}).length,
        qinBig: Object.keys(slot.qinBig || {}).length,
        qplBig: Object.keys(slot.qplBig || {}).length,
        pool: slot.pool,
        updAt: slot.updAt,
      };
    }
  }
  res.json({ ok: true, summary: summary });
});

app.listen(PORT, function() {
  console.log('[SERVER] Running on port ' + PORT);
  console.log('[SERVER] /odds /qin-qpl /stream /push /mqtt-status /health ready');
});

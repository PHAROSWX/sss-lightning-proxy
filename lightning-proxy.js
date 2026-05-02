/**
 * lightning-proxy.js — Blitzortung WebSocket Proxy
 * ─────────────────────────────────────────────────────────────────────────────
 * Runs on your VPS. Connects to Blitzortung on behalf of your server,
 * then relays strikes to all connected browser clients.
 *
 * Install:  npm install ws
 * Run:      node lightning-proxy.js
 * Or with pm2 (recommended): pm2 start lightning-proxy.js --name lightning
 *
 * The browser connects to: wss://yourdomain.com/lightning-ws
 * (configure your nginx to proxy that path to port 2345)
 */

'use strict';

const { WebSocketServer, WebSocket } = require('ws');

// ── Config ─────────────────────────────────────────────────────────────────
const PROXY_PORT      = 2345;          // port this proxy listens on
const BLITZORTUNG_URLS = [
  'wss://ws1.blitzortung.org/',
  'wss://ws7.blitzortung.org/',
  'wss://ws8.blitzortung.org/',
];

const SUBSCRIBE = JSON.stringify({
  west: -30, east: 50, north: 72, south: 28,
});
const RECONNECT_MS = 5000;
const PING_MS      = 15000;  // keepalive ping every 15s

// ── State ──────────────────────────────────────────────────────────────────
let blitzWs     = null;
let wsIndex     = 0;
let clients     = new Set();   // connected browser clients
let strikeCount = 0;
let reconnTimer = null;

// ── Browser WebSocket server ───────────────────────────────────────────────
const wss = new WebSocketServer({ port: PROXY_PORT });

wss.on('connection', function(ws, req) {
  clients.add(ws);
  console.log('[proxy] Client connected — total:', clients.size);

  ws.on('close', function() {
    clients.delete(ws);
    console.log('[proxy] Client disconnected — total:', clients.size);
  });

  ws.on('error', function(e) {
    clients.delete(ws);
  });

  // Send current stats immediately
  ws.send(JSON.stringify({ type: 'stats', count: strikeCount, clients: clients.size }));
});

console.log('[proxy] Browser WebSocket server listening on port', PROXY_PORT);

// ── Blitzortung upstream connection ───────────────────────────────────────
let pingTimer = null;

function connectBlitzortung() {
  if (reconnTimer) { clearTimeout(reconnTimer); reconnTimer = null; }
  if (pingTimer)   { clearInterval(pingTimer);  pingTimer   = null; }

  const url = BLITZORTUNG_URLS[wsIndex % BLITZORTUNG_URLS.length];
  console.log('[proxy] Connecting to Blitzortung:', url);

  blitzWs = new WebSocket(url, {
    headers: {
      'Origin':          'https://www.blitzortung.org',
      'User-Agent':      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36',
      'Accept-Language': 'en-US,en;q=0.9',
      'Cache-Control':   'no-cache',
      'Pragma':          'no-cache',
    },
    rejectUnauthorized: false,
  });

  blitzWs.on('open', function() {
    console.log('[proxy] Blitzortung connected — subscribing');
    blitzWs.send(SUBSCRIBE);
    // Keepalive ping every 15s — Blitzortung drops idle connections
    pingTimer = setInterval(function() {
      if (blitzWs && blitzWs.readyState === WebSocket.OPEN) {
        try { blitzWs.ping(); } catch(_) {}
      }
    }, 15000);
  });

  blitzWs.on('pong', function() { /* alive */ });

  blitzWs.on('message', function(raw) {
    try {
      var data = JSON.parse(raw.toString());
      var lat, lng, pairs = [];

      // Parse all known Blitzortung message formats
      if (data.lat !== undefined) {
        lat = parseFloat(data.lat);
        lng = parseFloat(data.lon ?? data.lng);
        if (!isNaN(lat) && !isNaN(lng)) pairs.push({ lat, lng });
      } else if (Array.isArray(data) && data.length >= 2) {
        lat = parseFloat(data[0]); lng = parseFloat(data[1]);
        if (!isNaN(lat) && !isNaN(lng)) pairs.push({ lat, lng });
      } else if (data.strikes && Array.isArray(data.strikes)) {
        data.strikes.forEach(function(s) {
          var la = parseFloat(s.lat ?? s[0]);
          var lo = parseFloat(s.lon ?? s.lng ?? s[1]);
          if (!isNaN(la) && !isNaN(lo)) pairs.push({ lat: la, lng: lo });
        });
      }

      if (!pairs.length) return;

      // Relay to all connected clients
      var now = Date.now();
      var msg = JSON.stringify({ type: 'strikes', time: now, strikes: pairs });
      strikeCount += pairs.length;

      clients.forEach(function(client) {
        if (client.readyState === WebSocket.OPEN) {
          client.send(msg);
        }
      });

      if (strikeCount % 100 === 0) {
        console.log('[proxy] Relayed', strikeCount, 'strikes total, clients:', clients.size);
      }
    } catch(e) {}
  });

  blitzWs.on('close', function(code) {
    console.log('[proxy] Blitzortung disconnected code=' + code + ' — reconnecting');
    if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
    wsIndex++;
    reconnTimer = setTimeout(connectBlitzortung, RECONNECT_MS);
  });

  blitzWs.on('error', function(e) {
    console.warn('[proxy] Blitzortung error:', e.message);
    if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
    wsIndex++;
    try { blitzWs.close(); } catch(_) {}
  });
}

connectBlitzortung();

// ── Nginx config (add this to your server block) ───────────────────────────
/*

location /lightning-ws {
    proxy_pass         http://127.0.0.1:2345;
    proxy_http_version 1.1;
    proxy_set_header   Upgrade    $http_upgrade;
    proxy_set_header   Connection "upgrade";
    proxy_set_header   Host       $host;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
}

*/

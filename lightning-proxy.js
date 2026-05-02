/**
 * lightning-proxy.js — Blitzortung MQTT → WebSocket Proxy
 * ─────────────────────────────────────────────────────────────────────────────
 * Connects to the public Blitzortung MQTT server (blitzortung.ha.sed.pl:1883)
 * which is specifically designed for third-party app integration.
 * Relays strikes to browser clients over WebSocket.
 *
 * Install:  npm install mqtt ws
 * Run:      node lightning-proxy.js
 * PM2:      pm2 start lightning-proxy.js --name lightning
 */

'use strict';

const mqtt = require('mqtt');
const { WebSocketServer, WebSocket } = require('ws');

// ── Config ─────────────────────────────────────────────────────────────────
const PROXY_PORT = process.env.PORT || 2345;
const MQTT_HOST  = 'mqtt://blitzortung.ha.sed.pl:1883';
const MQTT_TOPIC = 'blitzortung/1.1/#';

// ── State ──────────────────────────────────────────────────────────────────
let clients     = new Set();
let strikeCount = 0;

// ── Browser WebSocket server ───────────────────────────────────────────────
// WebSocket server attached to HTTP server below

wss.on('connection', function(ws) {
  clients.add(ws);
  console.log('[proxy] Browser client connected — total:', clients.size);
  ws.on('close',  function()  { clients.delete(ws); console.log('[proxy] Client gone — total:', clients.size); });
  ws.on('error',  function()  { clients.delete(ws); });
  ws.send(JSON.stringify({ type: 'stats', strikeCount, clients: clients.size }));
});

console.log('[proxy] WebSocket server listening on port', PROXY_PORT);

// Railway requires an HTTP response on the same port for health checks
// The WebSocketServer handles upgrade requests; HTTP requests get a simple 200
const http = require('http');
const httpServer = http.createServer(function(req, res) {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('SSS Lightning Proxy — ' + strikeCount + ' strikes relayed\n');
});
// Attach WebSocket server to the http server so both share the same port
const wss2 = new WebSocketServer({ server: httpServer });
wss2.on('connection', wss.emit.bind(wss, 'connection'));
httpServer.listen(PROXY_PORT, '0.0.0.0', function() {
  console.log('[proxy] HTTP+WS server on port', PROXY_PORT);
});

// ── MQTT connection ────────────────────────────────────────────────────────
const mqttClient = mqtt.connect(MQTT_HOST, {
  clientId:        'sss_proxy_' + Math.random().toString(16).slice(2, 8),
  clean:           true,
  reconnectPeriod: 5000,
  connectTimeout:  10000,
});

mqttClient.on('connect', function() {
  console.log('[proxy] MQTT connected to', MQTT_HOST);
  mqttClient.subscribe(MQTT_TOPIC, function(err) {
    if (err) console.error('[proxy] Subscribe error:', err.message);
    else     console.log('[proxy] Subscribed to', MQTT_TOPIC);
  });
});

mqttClient.on('message', function(topic, payload) {
  try {
    var data = JSON.parse(payload.toString());
    var lat  = parseFloat(data.lat);
    var lon  = parseFloat(data.lon);
    if (isNaN(lat) || isNaN(lon)) return;

    strikeCount++;
    if (strikeCount % 100 === 0)
      console.log('[proxy] Relayed', strikeCount, 'strikes | clients:', clients.size);

    var msg = JSON.stringify({ type: 'strikes', time: Date.now(), strikes: [{ lat, lng: lon }] });
    clients.forEach(function(client) {
      if (client.readyState === WebSocket.OPEN) client.send(msg);
    });
  } catch(e) {}
});

mqttClient.on('reconnect', function() { console.log('[proxy] MQTT reconnecting…'); });
mqttClient.on('error',     function(e) { console.error('[proxy] MQTT error:', e.message); });
mqttClient.on('offline',   function()  { console.log('[proxy] MQTT offline'); });

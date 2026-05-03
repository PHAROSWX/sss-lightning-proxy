/**
 * lightning-proxy.js — Blitzortung MQTT → WebSocket Proxy
 * Buffers 24h of strikes server-side so new clients get history on connect.
 * Install:  npm install mqtt ws
 * Run:      node lightning-proxy.js
 */

'use strict';

const http  = require('http');
const mqtt  = require('mqtt');
const { WebSocketServer, WebSocket } = require('ws');

const PROXY_PORT     = process.env.PORT || 8080;
const MQTT_HOST      = 'mqtt://blitzortung.ha.sed.pl:1883';
const MQTT_TOPIC     = 'blitzortung/1.1/#';
const MAX_HISTORY_MS = 24 * 60 * 60 * 1000; // 24 hours
const MAX_HISTORY    = 500000;               // ~24h of global strikes
const BATCH_SIZE     = 500;                  // send history in chunks

let clients     = new Set();
let strikeCount = 0;
let history     = [];  // { lat, lng, time }

// Prune history older than 24h
function pruneHistory() {
  var cutoff = Date.now() - MAX_HISTORY_MS;
  var i = 0;
  while (i < history.length && history[i].time < cutoff) i++;
  if (i > 0) history.splice(0, i);
  if (history.length > MAX_HISTORY) history.splice(0, history.length - MAX_HISTORY);
}

// HTTP server (Railway health check)
const server = http.createServer(function(req, res) {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('SSS Lightning Proxy\nStrikes relayed: ' + strikeCount +
          '\nHistory buffer: ' + history.length + '\n');
});

// WebSocket server
const wss = new WebSocketServer({ server });

wss.on('connection', function(ws) {
  clients.add(ws);
  console.log('[proxy] Client connected — total:', clients.size,
              '| history buffer:', history.length);

  ws.on('close', function() {
    clients.delete(ws);
    console.log('[proxy] Client gone — total:', clients.size);
  });
  ws.on('error', function() { clients.delete(ws); });

  // Send full history to the new client in batches
  pruneHistory();
  var h = history.slice(); // snapshot
  var sent = 0;

  function sendNextBatch() {
    if (ws.readyState !== WebSocket.OPEN) return;
    var batch = h.slice(sent, sent + BATCH_SIZE);
    if (!batch.length) {
      // All history sent — signal done
      ws.send(JSON.stringify({ type: 'history_done', total: h.length }));
      return;
    }
    ws.send(JSON.stringify({ type: 'strikes', time: null, strikes: batch, historical: true }));
    sent += batch.length;
    // Small delay between batches to avoid overwhelming the client
    setTimeout(sendNextBatch, 20);
  }

  sendNextBatch();
});

server.listen(PROXY_PORT, '0.0.0.0', function() {
  console.log('[proxy] HTTP+WS listening on port', PROXY_PORT);
});

// MQTT
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

    var strike = { lat, lng: lon, time: Date.now() };
    history.push(strike);
    strikeCount++;

    // Prune every 1000 strikes
    if (strikeCount % 1000 === 0) pruneHistory();

    if (strikeCount % 1000 === 0)
      console.log('[proxy] Relayed', strikeCount, '| buffer:', history.length,
                  '| clients:', clients.size);

    // Relay live to connected clients
    var msg = JSON.stringify({ type: 'strikes', time: strike.time, strikes: [{ lat, lng: lon }] });
    clients.forEach(function(client) {
      if (client.readyState === WebSocket.OPEN) client.send(msg);
    });
  } catch(e) {}
});

mqttClient.on('reconnect', function() { console.log('[proxy] MQTT reconnecting…'); });
mqttClient.on('error',     function(e) { console.error('[proxy] MQTT error:', e.message); });
mqttClient.on('offline',   function()  { console.log('[proxy] MQTT offline'); });

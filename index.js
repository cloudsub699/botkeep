'use strict';

const http = require('http');
const net = require('net');
const WebSocket = require('ws');

// =========================
// Configuration
// =========================

const PORT = Number(
    process.env.PORT ||
    process.env.SERVER_PORT ||
    3000
);

const UUID = (
    process.env.UUID ||
    '27116477-2380-4509-a9b2-5d1f4e0d3e07'
).toLowerCase();

const MAX_CONNECTIONS = Number(process.env.MAX_CONNECTIONS || 50);
const CONNECT_TIMEOUT = Number(process.env.CONNECT_TIMEOUT || 10000);
const IDLE_TIMEOUT = Number(process.env.IDLE_TIMEOUT || 120000);
const MAX_PAYLOAD = Number(process.env.MAX_PAYLOAD || 1024 * 1024);

let activeConnections = 0;

// =========================
// Helpers
// =========================

function toUUID(bytes) {
    if (!Buffer.isBuffer(bytes) || bytes.length !== 16) return null;
    const hex = [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`.toLowerCase();
}

function safeClose(ws, code = 1000, reason = '') {
    try {
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
            ws.close(code, reason);
        }
    } catch (_) {}
}

function destroySocket(socket) {
    if (!socket) return;
    try {
        if (!socket.destroyed) socket.destroy();
    } catch (_) {}
}

// =========================
// HTTP server (Only prints config)
// =========================

const server = http.createServer((req, res) => {
    const host = req.headers.host || 'localhost';

    const vlessLink =
        `vless://${UUID}@${host}` +
        `?encryption=none` +
        `&security=none` +
        `&type=ws` +
        `&host=${host}` +
        `&path=%2F` +
        `#VLESS-Proxy`;

    res.writeHead(200, {
        'Content-Type': 'text/plain; charset=utf-8',
        'Cache-Control': 'no-store'
    });

    // خروجی فقط شامل کانفیگ است
    res.end(vlessLink);
});

// =========================
// WebSocket server
// =========================

const wss = new WebSocket.Server({
    server,
    maxPayload: MAX_PAYLOAD,
    clientTracking: true,
    verifyClient: (_info, done) => {
        if (activeConnections >= MAX_CONNECTIONS) {
            done(false, 503, 'Server busy');
            return;
        }
        done(true);
    }
});

wss.on('connection', (ws, req) => {
    activeConnections++;
    let targetSocket = null;
    let cleanedUp = false;
    let connected = false;
    let idleTimer = null;
    let connectTimer = null;

    function cleanup() {
        if (cleanedUp) return;
        cleanedUp = true;
        if (idleTimer) { clearTimeout(idleTimer); idleTimer = null; }
        if (connectTimer) { clearTimeout(connectTimer); connectTimer = null; }
        destroySocket(targetSocket);
        targetSocket = null;
        if (activeConnections > 0) activeConnections--;
    }

    function closeConnection(code = 1000, reason = '') {
        cleanup();
        safeClose(ws, code, reason);
    }

    function resetIdleTimer() {
        if (idleTimer) clearTimeout(idleTimer);
        idleTimer = setTimeout(() => {
            closeConnection(1000, 'Idle timeout');
        }, IDLE_TIMEOUT);
    }

    ws.on('error', () => cleanup());
    ws.on('close', () => cleanup());
    ws.on('pong', () => resetIdleTimer());

    function handleVless(buffer) {
        if (cleanedUp) return;
        try {
            if (!Buffer.isBuffer(buffer)) buffer = Buffer.from(buffer);
            if (buffer.length < 24) { closeConnection(1002, 'Invalid VLESS request'); return; }

            const version = buffer[0];
            if (version !== 0x00) { closeConnection(1002, 'Unsupported version'); return; }

            const clientUUID = toUUID(buffer.subarray(1, 17));
            if (!clientUUID || clientUUID !== UUID) { closeConnection(1008, 'Invalid UUID'); return; }

            const addonsLength = buffer[17];
            let offset = 18 + addonsLength;
            if (buffer.length <= offset) { closeConnection(1002, 'Invalid addons'); return; }

            const command = buffer[offset++];
            if (command !== 0x01) { closeConnection(1003, 'Only TCP supported'); return; }

            if (buffer.length < offset + 2) { closeConnection(1002, 'Missing port'); return; }
            const targetPort = (buffer[offset] << 8) | buffer[offset + 1];
            offset += 2;

            if (targetPort < 1 || targetPort > 65535) { closeConnection(1002, 'Invalid port'); return; }
            if (buffer.length <= offset) { closeConnection(1002, 'Missing

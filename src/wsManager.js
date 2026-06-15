const { WebSocketServer } = require("ws");

let wss = null;

/**
 * Attach a WebSocket server to an existing HTTP server.
 * @param {http.Server} httpServer
 */
function init(httpServer) {
  wss = new WebSocketServer({ server: httpServer });

  wss.on("connection", (ws, req) => {
    const ip = req.socket.remoteAddress;
    console.log(`[WS] Client connected from ${ip} — total: ${wss.clients.size}`);

    ws.send(JSON.stringify({ event: "connected", message: "Job Queue WebSocket ready" }));

    ws.on("close", () => {
      console.log(`[WS] Client disconnected — remaining: ${wss.clients.size}`);
    });

    ws.on("error", (err) => {
      console.error("[WS] Client error:", err.message);
    });
  });

  console.log("✅ WebSocket server initialized");
  return wss;
}

/**
 * Broadcast a job event to all connected WebSocket clients.
 * @param {object} payload
 */
function broadcast(payload) {
  if (!wss) return;

  const message = JSON.stringify({ ...payload, timestamp: new Date().toISOString() });
  let sent = 0;

  wss.clients.forEach((client) => {
    if (client.readyState === 1 /* OPEN */) {
      client.send(message);
      sent++;
    }
  });

  if (sent > 0) {
    console.log(`[WS] Broadcast to ${sent} client(s): ${payload.event} job=${payload.jobId}`);
  }
}

module.exports = { init, broadcast };

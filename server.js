/**
 * Stick Fight Online - WebSocket relay + game host server
 *
 * 同一WiFi:  npm start
 * 別WiFi:    npm run tunnel:jp  （Cloudflare東京経由・おすすめ）
 *            npm run tunnel      （海外経由・遅い）
 *
 * Open: http://localhost:8765
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { spawn } = require("child_process");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8765;
const HOST = "0.0.0.0";
const TUNNEL_MODE = process.argv.includes("--tunnel-cf")
  ? "cloudflare"
  : process.argv.includes("--tunnel") || process.env.TUNNEL === "1"
    ? "localtunnel"
    : null;
const USE_TUNNEL = !!TUNNEL_MODE;
const MAX_ROOMS = 200;
const ROOM_TTL_MS = 30 * 60 * 1000;

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".svg": "image/svg+xml",
  ".json": "application/json; charset=utf-8",
  ".ico": "image/x-icon",
};

const rooms = new Map();
let tunnelInfo = { http: null, ws: null, provider: null };

function getLocalIPs() {
  const ips = [];
  const nets = os.networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const net of nets[name] || []) {
      if (net.family === "IPv4" && !net.internal) ips.push(net.address);
    }
  }
  return ips;
}

function genCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return rooms.has(code) ? genCode() : code;
}

function send(ws, data) {
  if (ws.readyState === 1) ws.send(JSON.stringify(data));
}

function broadcast(room, data, exceptWs) {
  room.members.forEach((m) => {
    if (m.ws !== exceptWs) send(m.ws, data);
  });
}

function roomInfo(room) {
  return {
    code: room.code,
    hostSlot: room.hostSlot,
    members: room.members.map((m) => ({
      slot: m.slot,
      name: m.name,
      ready: m.ready,
      isHost: m.slot === room.hostSlot,
    })),
    playing: room.playing,
  };
}

function cleanupRooms() {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.lastActive > ROOM_TTL_MS) rooms.delete(code);
  }
}
setInterval(cleanupRooms, 60000);

function serveStatic(req, res) {
  let urlPath = req.url.split("?")[0];
  if (urlPath === "/") urlPath = "/index.html";
  const rel = urlPath.replace(/^\/+/, "");
  const filePath = path.normalize(path.join(__dirname, rel));
  if (!filePath.startsWith(__dirname)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end("Not found");
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  if (req.url === "/api/info" || req.url.startsWith("/api/info?")) {
    res.writeHead(200, {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    });
    res.end(
      JSON.stringify({
        port: PORT,
        ips: getLocalIPs(),
        tunnel: tunnelInfo.http,
        tunnelWs: tunnelInfo.ws,
        tunnelEnabled: USE_TUNNEL,
        tunnelProvider: tunnelInfo.provider,
      })
    );
    return;
  }
  serveStatic(req, res);
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  let member = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.type === "create") {
      if (rooms.size >= MAX_ROOMS) {
        send(ws, { type: "error", message: "サーバーが満員です" });
        return;
      }
      const code = genCode();
      const name = (msg.name || "Host").slice(0, 12);
      member = { ws, slot: 0, name, ready: true, roomCode: code };
      const room = {
        code,
        hostSlot: 0,
        members: [member],
        playing: false,
        lastActive: Date.now(),
      };
      rooms.set(code, room);
      send(ws, { type: "joined", ...roomInfo(room), yourSlot: 0 });
      return;
    }

    if (msg.type === "join") {
      const code = (msg.code || "").toUpperCase().slice(0, 4);
      const room = rooms.get(code);
      if (!room) {
        send(ws, { type: "error", message: "ルームが見つかりません" });
        return;
      }
      if (room.playing) {
        send(ws, { type: "error", message: "試合中のルームです" });
        return;
      }
      if (room.members.length >= 4) {
        send(ws, { type: "error", message: "ルームが満員です" });
        return;
      }
      const used = new Set(room.members.map((m) => m.slot));
      let slot = 0;
      while (used.has(slot) && slot < 4) slot++;
      const name = (msg.name || `P${slot + 1}`).slice(0, 12);
      member = { ws, slot, name, ready: false, roomCode: code };
      room.members.push(member);
      room.lastActive = Date.now();
      send(ws, { type: "joined", ...roomInfo(room), yourSlot: slot });
      broadcast(room, { type: "lobby", ...roomInfo(room) });
      return;
    }

    if (!member) return;
    const room = rooms.get(member.roomCode || msg.room);
    if (!room) return;
    room.lastActive = Date.now();

    if (msg.type === "ready") {
      member.ready = !!msg.ready;
      broadcast(room, { type: "lobby", ...roomInfo(room) });
      return;
    }

    if (msg.type === "start") {
      if (member.slot !== room.hostSlot) return;
      const humans = room.members.length;
      if (humans < 2) {
        send(ws, { type: "error", message: "2人以上必要です" });
        return;
      }
      room.playing = true;
      const startMsg = {
        type: "start",
        stageSeed: Date.now(),
        humanSlots: room.members.map((m) => m.slot),
        roomSettings: msg.roomSettings || null,
      };
      room.members.forEach((m) => send(m.ws, startMsg));
      return;
    }

    if (msg.type === "input") {
      if (member.slot !== msg.slot) return;
      const host = room.members.find((m) => m.slot === room.hostSlot);
      if (host) {
        send(host.ws, { type: "input", slot: msg.slot, input: msg.input, seq: msg.seq || 0 });
      }
      return;
    }

    if (msg.type === "state") {
      if (member.slot !== room.hostSlot) return;
      broadcast(room, { type: "state", state: msg.state }, ws);
      return;
    }

    if (msg.type === "chat") {
      broadcast(room, {
        type: "chat",
        slot: member.slot,
        name: member.name,
        text: (msg.text || "").slice(0, 80),
      });
    }
  });

  ws.on("close", () => {
    if (!member) return;
    for (const [code, room] of rooms) {
      const idx = room.members.findIndex((m) => m.ws === ws);
      if (idx === -1) continue;
      const wasHost = member.slot === room.hostSlot;
      room.members.splice(idx, 1);
      if (room.members.length === 0) {
        rooms.delete(code);
      } else {
        if (wasHost) {
          room.hostSlot = room.members[0].slot;
          room.members[0].ready = true;
          room.playing = false;
        }
        broadcast(room, { type: "lobby", ...roomInfo(room) });
        if (room.playing) broadcast(room, { type: "host_left" });
      }
      break;
    }
  });
});

async function startTunnel() {
  try {
    const localtunnel = require("localtunnel");
    const tunnel = await localtunnel({ port: Number(PORT) });
    tunnelInfo.http = tunnel.url;
    tunnelInfo.ws = tunnel.url.replace(/^http:/, "ws:").replace(/^https:/, "wss:");
    tunnelInfo.provider = "localtunnel";
    console.log("");
    console.log("  *** ポート開放不要（海外経由・遅い場合あり）***");
    console.log(`  公開URL:     ${tunnelInfo.http}`);
    console.log(`  友達に共有:  ${tunnelInfo.ws}`);
    console.log("  → 遅い場合は Ctrl+C → npm run tunnel:jp を試してください");
    tunnel.on("close", () => {
      tunnelInfo = { http: null, ws: null, provider: null };
      console.log("Tunnel closed. Reconnecting in 5s...");
      setTimeout(startTunnel, 5000);
    });
  } catch (e) {
    console.warn("Tunnel failed:", e.message);
    console.warn("Try: npm install   then: npm run tunnel:jp");
  }
}

function startCloudflareTunnel() {
  console.log("\n  Cloudflare Tunnel 起動中（東京など国内エッジ経由）...");
  const cf = spawn(
    "npx",
    ["--yes", "cloudflared", "tunnel", "--url", `http://127.0.0.1:${PORT}`],
    { shell: true, stdio: ["ignore", "pipe", "pipe"] }
  );
  const onData = (chunk) => {
    const text = chunk.toString();
    process.stderr.write(text);
    const m = text.match(/https:\/\/[a-z0-9-]+\.trycloudflare\.com/i);
    if (m && !tunnelInfo.http) {
      tunnelInfo.http = m[0];
      tunnelInfo.ws = m[0].replace(/^https:/, "wss:");
      tunnelInfo.provider = "cloudflare";
      console.log("");
      console.log("  *** ポート開放不要・日本向け（Cloudflare）***");
      console.log(`  公開URL:     ${tunnelInfo.http}`);
      console.log(`  友達に共有:  ${tunnelInfo.ws}`);
      console.log("  → 友達はサーバー欄に上のWebSocket URLを入力");
    }
  };
  cf.stdout.on("data", onData);
  cf.stderr.on("data", onData);
  cf.on("close", () => {
    tunnelInfo = { http: null, ws: null, provider: null };
    console.log("Cloudflare tunnel closed. Reconnecting in 5s...");
    setTimeout(startCloudflareTunnel, 5000);
  });
  cf.on("error", (err) => {
    console.warn("cloudflared failed:", err.message);
    console.warn("初回は npx が cloudflared をダウンロードします。少し待ってください。");
  });
}

server.on("error", (err) => {
  if (err.code === "EADDRINUSE") {
    console.error(`\nポート ${PORT} は既に使用中です。`);
    console.error("別のウィンドウで npm start が動いている場合:");
    console.error("  1. そのウィンドウで Ctrl+C を押して止める");
    console.error("  2. もう一度 npm run tunnel を実行");
    console.error("\n同じWiFiの友達とだけ遊ぶなら、今動いている npm start のままでOKです。");
    process.exit(1);
  }
  throw err;
});

server.listen(PORT, HOST, () => {
  const ips = getLocalIPs();
  console.log("Stick Fight Online server running");
  console.log(`  Game:      http://localhost:${PORT}`);
  ips.forEach((ip) => console.log(`  LAN game:  http://${ip}:${PORT}`));
  if (USE_TUNNEL) {
    if (TUNNEL_MODE === "cloudflare") startCloudflareTunnel();
    else startTunnel();
  } else {
    console.log("");
    console.log("  別WiFiの友達と遊ぶ（ポート開放不要）:");
    console.log("    npm run tunnel:jp  ← おすすめ（Cloudflare・東京経由）");
    console.log("    npm run tunnel     ← 海外経由（遅い場合あり）");
  }
});

// Servidor de salas de Gnomo: el anfitrión publica el estado de la partida
// y los demás móviles lo reciben en directo (Server-Sent Events).
// Sin dependencias: solo Node.js. Las salas viven en memoria y se borran solas.
const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 10000;
const ROOM_TTL_MS = 12 * 60 * 60 * 1000; // 12 h sin actividad
const MAX_BODY = 200 * 1024;
const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ"; // sin I ni O para no confundir

/** code -> { hostKey, state, updated, clients:Set<res> } */
const rooms = new Map();

function newCode() {
  for (let tries = 0; tries < 50; tries++) {
    let c = "";
    for (let i = 0; i < 4; i++) c += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!rooms.has(c)) return c;
  }
  throw new Error("no code");
}

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type,X-Host-Key");
  res.setHeader("Access-Control-Max-Age", "86400");
}

function json(res, status, obj) {
  cors(res);
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on("data", (c) => {
      size += c.length;
      if (size > MAX_BODY) { reject(new Error("too big")); req.destroy(); return; }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function broadcast(room) {
  const msg = `data: ${JSON.stringify({ state: room.state, updated: room.updated })}\n\n`;
  for (const res of room.clients) res.write(msg);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  const parts = url.pathname.split("/").filter(Boolean); // ["rooms", CODE, "events"]

  if (req.method === "OPTIONS") { cors(res); res.writeHead(204); return res.end(); }

  if (url.pathname === "/" || url.pathname === "/health") {
    return json(res, 200, { ok: true, rooms: rooms.size });
  }

  // Crear sala
  if (req.method === "POST" && parts.length === 1 && parts[0] === "rooms") {
    // Si el anfitrión ya tenía sala (p. ej. el servidor se reinició), la recupera con el mismo código.
    let want = {};
    try { want = JSON.parse((await readBody(req)) || "{}"); } catch (e) {}
    const reuse = typeof want.code === "string" && /^[A-Z]{4}$/.test(want.code) && typeof want.hostKey === "string" && want.hostKey.length >= 16;
    if (reuse && rooms.has(want.code) && rooms.get(want.code).hostKey === want.hostKey) return json(res, 200, { code: want.code, hostKey: want.hostKey });
    const code = reuse && !rooms.has(want.code) ? want.code : newCode();
    const hostKey = reuse && code === want.code ? want.hostKey : crypto.randomBytes(18).toString("base64url");
    rooms.set(code, { hostKey, state: null, updated: Date.now(), clients: new Set() });
    return json(res, 201, { code, hostKey });
  }

  if (parts[0] !== "rooms" || !parts[1]) return json(res, 404, { error: "No existe" });
  const code = parts[1].toUpperCase();
  const room = rooms.get(code);
  if (!room) return json(res, 404, { error: "Esa sala no existe o ya ha terminado." });

  // Publicar estado (solo anfitrión)
  if (req.method === "POST" && parts[2] === "state") {
    if (req.headers["x-host-key"] !== room.hostKey) return json(res, 403, { error: "Solo el anfitrión puede cambiar la partida." });
    try {
      const body = await readBody(req);
      room.state = JSON.parse(body);
      room.updated = Date.now();
      broadcast(room);
      return json(res, 200, { ok: true, viewers: room.clients.size });
    } catch (e) {
      return json(res, 400, { error: "Datos no válidos." });
    }
  }

  // Cerrar sala (solo anfitrión)
  if (req.method === "POST" && parts[2] === "close") {
    if (req.headers["x-host-key"] !== room.hostKey) return json(res, 403, { error: "Solo el anfitrión puede cerrar la sala." });
    for (const c of room.clients) { c.write(`event: closed\ndata: {}\n\n`); c.end(); }
    rooms.delete(code);
    return json(res, 200, { ok: true });
  }

  // Seguir la partida en directo
  if (req.method === "GET" && parts[2] === "events") {
    cors(res);
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-store",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    res.write(`retry: 3000\n`);
    res.write(`data: ${JSON.stringify({ state: room.state, updated: room.updated })}\n\n`);
    room.clients.add(res);
    req.on("close", () => room.clients.delete(res));
    return;
  }

  if (req.method === "GET" && !parts[2]) {
    return json(res, 200, { state: room.state, updated: room.updated, viewers: room.clients.size });
  }

  return json(res, 404, { error: "No existe" });
});

// Latido para que las conexiones no se corten y limpieza de salas viejas
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.updated > ROOM_TTL_MS) {
      for (const c of room.clients) { c.write(`event: closed\ndata: {}\n\n`); c.end(); }
      rooms.delete(code);
    } else {
      for (const c of room.clients) c.write(`: ping\n\n`);
    }
  }
}, 25000);

server.listen(PORT, () => console.log(`Gnomo salas escuchando en ${PORT}`));

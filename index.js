const {
  default: makeWASocket,
  DisconnectReason,
  makeCacheableSignalKeyStore,
  fetchLatestBaileysVersion,
  jidNormalizedUser,
  isJidGroup,
  proto,
} = require("@whiskeysockets/baileys");
const { initAuthCreds, BufferJSON } = require("@whiskeysockets/baileys");
const express = require("express");
const cors = require("cors");
const qrcode = require("qrcode");
const Anthropic = require("@anthropic-ai/sdk");
const Redis = require("ioredis");
const pino = require("pino");

// ─── Config ───────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
const REDIS_URL = process.env.REDIS_URL || "";
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";
const API_TOKEN = process.env.BOT_API_TOKEN || "dev-token";
const MAX_MSGS_PER_CHAT = 50;

const logger = pino({ level: "warn" });
const anthropic = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;

// ─── Redis ────────────────────────────────────────────────────────────────────
let redis = null;
if (REDIS_URL) {
  redis = new Redis(REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 2 });
  redis.connect().catch(() => { redis = null; });
}

async function rGet(key) {
  if (!redis) return null;
  try { return await redis.get(key); } catch { return null; }
}
async function rSet(key, val, ttl = 0) {
  if (!redis) return;
  try {
    if (ttl) await redis.setex(key, ttl, val);
    else await redis.set(key, val);
  } catch {}
}
async function rDel(key) {
  if (!redis) return;
  try { await redis.del(key); } catch {}
}

// ─── Baileys auth state stored in Redis ──────────────────────────────────────
async function useRedisAuthState(redisPrefix = "wa_auth") {
  const KEY = `${redisPrefix}:creds`;
  const STORE_KEY = `${redisPrefix}:keys`;

  const readData = async (k) => {
    const raw = await rGet(`${STORE_KEY}:${k}`);
    if (!raw) return null;
    return JSON.parse(raw, BufferJSON.reviver);
  };
  const writeData = async (k, data) => {
    await rSet(`${STORE_KEY}:${k}`, JSON.stringify(data, BufferJSON.replacer));
  };
  const removeData = async (k) => {
    await rDel(`${STORE_KEY}:${k}`);
  };

  let creds;
  const rawCreds = await rGet(KEY);
  if (rawCreds) {
    creds = JSON.parse(rawCreds, BufferJSON.reviver);
  } else {
    creds = initAuthCreds();
  }

  return {
    state: {
      creds,
      keys: makeCacheableSignalKeyStore(
        {
          get: async (type, ids) => {
            const data = {};
            await Promise.all(
              ids.map(async (id) => {
                let value = await readData(`${type}-${id}`);
                if (type === "app-state-sync-key" && value) {
                  value = proto.Message.AppStateSyncKeyData.fromObject(value);
                }
                data[id] = value;
              })
            );
            return data;
          },
          set: async (data) => {
            const tasks = [];
            for (const category of Object.keys(data)) {
              for (const id of Object.keys(data[category])) {
                const value = data[category][id];
                tasks.push(value ? writeData(`${category}-${id}`, value) : removeData(`${category}-${id}`));
              }
            }
            await Promise.all(tasks);
          },
        },
        logger
      ),
    },
    saveCreds: async () => {
      await rSet(KEY, JSON.stringify(creds, BufferJSON.replacer));
    },
  };
}

// ─── In-memory state ──────────────────────────────────────────────────────────
let sock = null;
let currentQR = null;      // base64 png
let status = "disconnected"; // disconnected | connecting | qr | connected
const chatStore = new Map(); // jid -> { name, lastMsg, unread, msgs[] }
const sseClients = new Set();

function broadcast(event, data) {
  const payload = `data: ${JSON.stringify({ event, data })}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch { sseClients.delete(res); }
  }
}

function ensureChat(jid, name = "") {
  if (!chatStore.has(jid)) {
    chatStore.set(jid, { jid, name: name || jid.split("@")[0], lastMsg: null, unread: 0, msgs: [] });
  }
  return chatStore.get(jid);
}

function pushMessage(jid, msg) {
  const chat = ensureChat(jid);
  chat.msgs.push(msg);
  if (chat.msgs.length > MAX_MSGS_PER_CHAT) chat.msgs.shift();
  chat.lastMsg = msg;
  if (!msg.fromMe) chat.unread++;
  broadcast("message", { jid, msg });
}

// ─── Baileys connection ───────────────────────────────────────────────────────
async function startSocket() {
  status = "connecting";
  broadcast("status", { status });

  const { version } = await fetchLatestBaileysVersion();

  let authState, saveCreds;
  if (redis) {
    const r = await useRedisAuthState("wa_reader");
    authState = r.state;
    saveCreds = r.saveCreds;
  } else {
    // fallback: in-memory only (lost on restart)
    const { makeInMemoryStore } = require("@whiskeysockets/baileys");
    const { state, saveCreds: sc } = await (async () => {
      const { initAuthCreds } = require("@whiskeysockets/baileys");
      const creds = initAuthCreds();
      return {
        state: { creds, keys: makeCacheableSignalKeyStore({ get: async () => ({}), set: async () => {} }, logger) },
        saveCreds: async () => {},
      };
    })();
    authState = state;
    saveCreds = sc;
  }

  sock = makeWASocket({
    version,
    logger,
    auth: authState,
    printQRInTerminal: false,
    browser: ["Two Travel Panel", "Chrome", "1.0"],
    getMessage: async () => ({ conversation: "" }),
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async ({ connection, lastDisconnect, qr }) => {
    if (qr) {
      status = "qr";
      currentQR = await qrcode.toDataURL(qr);
      broadcast("qr", { qr: currentQR });
    }
    if (connection === "open") {
      status = "connected";
      currentQR = null;
      broadcast("status", { status });
      console.log("✅ WhatsApp connected");
    }
    if (connection === "close") {
      const code = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = code !== DisconnectReason.loggedOut;
      status = "disconnected";
      broadcast("status", { status });
      console.log("❌ Disconnected, code:", code, "reconnect:", shouldReconnect);
      if (shouldReconnect) {
        setTimeout(startSocket, 3000);
      } else {
        // Logged out — clear credentials
        if (redis) {
          await rDel("wa_reader:creds");
          // Don't delete keys, just reconnect fresh
        }
        setTimeout(startSocket, 1000);
      }
    }
  });

  sock.ev.on("chats.set", ({ chats }) => {
    for (const c of chats) {
      ensureChat(c.id, c.name || c.id.split("@")[0]);
    }
  });

  sock.ev.on("contacts.update", (contacts) => {
    for (const c of contacts) {
      if (c.id && c.notify) {
        ensureChat(c.id, c.notify);
        chatStore.get(c.id).name = c.notify;
      }
    }
  });

  sock.ev.on("messages.upsert", async ({ messages: msgs, type }) => {
    if (type !== "notify") return;
    for (const m of msgs) {
      if (!m.message) continue;
      const jid = m.key.remoteJid;
      const fromMe = m.key.fromMe;
      const text =
        m.message?.conversation ||
        m.message?.extendedTextMessage?.text ||
        m.message?.imageMessage?.caption ||
        m.message?.videoMessage?.caption ||
        "[media]";

      const pushName = m.pushName || "";
      ensureChat(jid, pushName || jid.split("@")[0]);

      pushMessage(jid, {
        id: m.key.id,
        ts: m.messageTimestamp,
        fromMe,
        sender: fromMe ? "yo" : pushName || jid.split("@")[0],
        text,
      });
    }
  });

  sock.ev.on("messages.update", (updates) => {
    // read receipts etc — ignore for now
  });
}

// ─── Express API ──────────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

function auth(req, res, next) {
  const token = req.headers["x-api-token"] || req.query.token;
  if (token !== API_TOKEN) return res.status(401).json({ error: "unauthorized" });
  next();
}

// Status + QR
app.get("/status", auth, (req, res) => {
  res.json({ status, qr: status === "qr" ? currentQR : null });
});

// Logout / force re-auth
app.post("/logout", auth, async (req, res) => {
  if (sock) {
    try { await sock.logout(); } catch {}
    sock = null;
  }
  if (redis) await rDel("wa_reader:creds");
  status = "disconnected";
  setTimeout(startSocket, 500);
  res.json({ ok: true });
});

// Chat list
app.get("/chats", auth, (req, res) => {
  const list = Array.from(chatStore.values())
    .filter((c) => c.lastMsg)
    .sort((a, b) => (b.lastMsg?.ts || 0) - (a.lastMsg?.ts || 0))
    .slice(0, 50)
    .map(({ jid, name, lastMsg, unread }) => ({ jid, name, lastMsg, unread }));
  res.json(list);
});

// Messages in a chat
app.get("/chat/:jid/messages", auth, (req, res) => {
  const jid = decodeURIComponent(req.params.jid);
  const chat = chatStore.get(jid);
  if (!chat) return res.json([]);
  // Mark as read
  chat.unread = 0;
  res.json(chat.msgs);
});

// Send a message
app.post("/chat/:jid/send", auth, async (req, res) => {
  const jid = decodeURIComponent(req.params.jid);
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: "text required" });
  if (!sock || status !== "connected") return res.status(503).json({ error: "not connected" });
  try {
    await sock.sendMessage(jid, { text });
    pushMessage(jid, { id: Date.now().toString(), ts: Math.floor(Date.now() / 1000), fromMe: true, sender: "yo", text });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// AI suggest reply
app.post("/chat/:jid/suggest", auth, async (req, res) => {
  const jid = decodeURIComponent(req.params.jid);
  const { messages: contextMsgs, instruction } = req.body;
  if (!anthropic) return res.status(503).json({ error: "Anthropic not configured" });

  const chat = chatStore.get(jid);
  const recentMsgs = contextMsgs || chat?.msgs?.slice(-10) || [];

  const history = recentMsgs
    .map((m) => `${m.fromMe ? "Yo" : m.sender}: ${m.text}`)
    .join("\n");

  const systemPrompt = `Eres un asistente de Two Travel, empresa de concierge y viajes de lujo en Colombia.
Ayudas al equipo concierge a redactar respuestas profesionales, cálidas y en español para sus clientes.
Responde SOLO con el texto del mensaje, sin explicaciones adicionales.
Sé breve, cordial y profesional. Usa el tono de la conversación.`;

  try {
    const response = await anthropic.messages.create({
      model: "claude-haiku-5-5",
      max_tokens: 300,
      system: systemPrompt,
      messages: [
        {
          role: "user",
          content: `Conversación:\n${history}\n\n${instruction ? `Instrucción: ${instruction}\n\n` : ""}Redacta una respuesta apropiada para el último mensaje del cliente.`,
        },
      ],
    });
    res.json({ suggestion: response.content[0].text });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Server-Sent Events for real-time updates
app.get("/events", auth, (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  sseClients.add(res);
  // Send current status immediately
  res.write(`data: ${JSON.stringify({ event: "status", data: { status, qr: status === "qr" ? currentQR : null } })}\n\n`);

  req.on("close", () => sseClients.delete(res));
});

// Health check
app.get("/", (req, res) => res.json({ service: "bot-wa-reader", status }));

app.listen(PORT, () => {
  console.log(`🚀 bot-wa-reader running on port ${PORT}`);
  startSocket();
});

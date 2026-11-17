import {
  default as makeWASocket,
  DisconnectReason,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
} from "@whiskeysockets/baileys";
import express from "express";
import cors from "cors";
import qrcode from "qrcode";
import Anthropic from "@anthropic-ai/sdk";
import pino from "pino";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ─── Config ───────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";
const API_TOKEN = process.env.BOT_API_TOKEN || "dev-token";
const AUTH_DIR = process.env.AUTH_DIR || "./auth_info";
const MAX_MSGS = 50;

const logger = pino({ level: "warn" });
const anthropic = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;

// ─── State ────────────────────────────────────────────────────────────────────
let sock = null;
let currentQR = null;
let status = "disconnected";
const chatStore = new Map();
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
  if (chat.msgs.length > MAX_MSGS) chat.msgs.shift();
  chat.lastMsg = msg;
  if (!msg.fromMe) chat.unread++;
  broadcast("message", { jid, msg });
}

// ─── Baileys ──────────────────────────────────────────────────────────────────
async function startSocket() {
  status = "connecting";
  broadcast("status", { status });

  if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    logger,
    auth: state,
    printQRInTerminal: true,
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
      const loggedOut = code === DisconnectReason.loggedOut;
      status = "disconnected";
      broadcast("status", { status });
      console.log("❌ Disconnected, code:", code);
      if (loggedOut) {
        fs.rmSync(AUTH_DIR, { recursive: true, force: true });
      }
      setTimeout(startSocket, 2000);
    }
  });

  sock.ev.on("chats.set", ({ chats }) => {
    for (const c of chats) ensureChat(c.id, c.name || c.id.split("@")[0]);
  });

  sock.ev.on("contacts.update", (contacts) => {
    for (const c of contacts) {
      if (c.id && c.notify) {
        ensureChat(c.id, c.notify);
        chatStore.get(c.id).name = c.notify;
      }
    }
  });

  sock.ev.on("messages.upsert", ({ messages: msgs, type }) => {
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
        ts: Number(m.messageTimestamp),
        fromMe,
        sender: fromMe ? "yo" : pushName || jid.split("@")[0],
        text,
      });
    }
  });
}

// ─── Express ──────────────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

function auth(req, res, next) {
  const token = req.headers["x-api-token"] || req.query.token;
  if (token !== API_TOKEN) return res.status(401).json({ error: "unauthorized" });
  next();
}

app.get("/", (req, res) => res.json({ service: "bot-wa-reader", status }));

app.get("/qr", auth, (req, res) => {
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(`<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WhatsApp QR — Two Travel</title>
<style>
body{font-family:system-ui,sans-serif;background:#111;color:#eee;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;margin:0;gap:20px}
h1{font-size:1.2rem;color:#aaa}
#badge{padding:6px 16px;border-radius:999px;font-size:.9rem;font-weight:600}
.qr{background:#25d366;color:#000}.connecting{background:#555;color:#eee}.connected{background:#128c7e;color:#fff}.disconnected{background:#c00;color:#fff}
#qr-box{background:#fff;padding:16px;border-radius:12px;display:none}
#qr-box img{display:block;width:260px;height:260px}
#ok{display:none;font-size:1.5rem;color:#25d366}
#msg{font-size:.85rem;color:#aaa;text-align:center;max-width:320px}
</style></head><body>
<h1>🤖 Two Travel — WhatsApp Reader</h1>
<span id="badge" class="connecting">Conectando…</span>
<div id="qr-box"><img id="qr-img" src="" alt="QR"></div>
<div id="ok">✅ ¡WhatsApp conectado!</div>
<p id="msg">Esperando QR del servidor…</p>
<script>
const TOKEN="${API_TOKEN}";
async function poll(){
  try{
    const d=await fetch("/status?token="+TOKEN).then(r=>r.json());
    const badge=document.getElementById("badge"),qrBox=document.getElementById("qr-box"),
          qrImg=document.getElementById("qr-img"),ok=document.getElementById("ok"),msg=document.getElementById("msg");
    if(d.status==="qr"&&d.qr){
      badge.textContent="Escanea el QR";badge.className="badge qr";
      qrImg.src=d.qr;qrBox.style.display="block";ok.style.display="none";
      msg.textContent="Abre WhatsApp → Dispositivos vinculados → Vincular un dispositivo → escanea este código";
    }else if(d.status==="connected"){
      badge.textContent="Conectado ✓";badge.className="badge connected";
      qrBox.style.display="none";ok.style.display="block";
      msg.textContent="Ya puedes usar el Bot Asistente en el Concierge Panel";return;
    }else if(d.status==="connecting"){
      badge.textContent="Conectando…";badge.className="badge connecting";
      msg.textContent="Iniciando conexión con WhatsApp…";
    }else{
      badge.textContent=d.status;badge.className="badge disconnected";
      msg.textContent="Reconectando…";
    }
  }catch(e){msg.textContent="⏳ Reintentando…";}
  setTimeout(poll,3000);
}
poll();
</script></body></html>`);
});

app.get("/status", auth, (req, res) =>
  res.json({ status, qr: status === "qr" ? currentQR : null })
);

app.post("/logout", auth, async (req, res) => {
  try { if (sock) await sock.logout(); } catch {}
  sock = null;
  fs.rmSync(AUTH_DIR, { recursive: true, force: true });
  status = "disconnected"; currentQR = null;
  chatStore.clear();
  setTimeout(startSocket, 500);
  res.json({ ok: true });
});

app.get("/chats", auth, (req, res) => {
  const list = Array.from(chatStore.values())
    .filter(c => c.lastMsg)
    .sort((a, b) => (b.lastMsg?.ts || 0) - (a.lastMsg?.ts || 0))
    .slice(0, 60)
    .map(({ jid, name, lastMsg, unread }) => ({ jid, name, lastMsg, unread }));
  res.json(list);
});

app.get("/chat/:jid/messages", auth, (req, res) => {
  const jid = decodeURIComponent(req.params.jid);
  const chat = chatStore.get(jid);
  if (chat) chat.unread = 0;
  res.json(chat?.msgs || []);
});

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

app.post("/chat/:jid/suggest", auth, async (req, res) => {
  if (!anthropic) return res.status(503).json({ error: "Anthropic not configured" });
  const jid = decodeURIComponent(req.params.jid);
  const { messages: ctxMsgs } = req.body;
  const chat = chatStore.get(jid);
  const recent = ctxMsgs || chat?.msgs?.slice(-10) || [];
  const history = recent.map(m => `${m.fromMe ? "Yo" : m.sender}: ${m.text}`).join("\n");
  try {
    const r = await anthropic.messages.create({
      model: "claude-haiku-5-5",
      max_tokens: 300,
      system: "Eres asistente de Two Travel, empresa de concierge de lujo. Redacta respuestas breves, cálidas y profesionales en español para el equipo. Solo el texto del mensaje, sin explicaciones.",
      messages: [{ role: "user", content: `Conversación:\n${history}\n\nRedacta la próxima respuesta apropiada.` }],
    });
    res.json({ suggestion: r.content[0].text });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/events", auth, (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();
  sseClients.add(res);
  res.write(`data: ${JSON.stringify({ event: "status", data: { status, qr: status === "qr" ? currentQR : null } })}\n\n`);
  req.on("close", () => sseClients.delete(res));
});

app.listen(PORT, () => {
  console.log(`🚀 bot-wa-reader on port ${PORT}`);
  startSocket();
});

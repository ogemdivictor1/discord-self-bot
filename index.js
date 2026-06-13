const express = require('express');
const { HttpsProxyAgent } = require('https-proxy-agent');
require('dotenv').config();
const { Client } = require('discord.js-selfbot-v13');
const axios = require('axios');
const { Redis } = require('@upstash/redis');

// -------------------- REDIS --------------------
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// -------------------- PROXY (optional) --------------------
const proxyUrl = process.env.PROXY_URL;
let httpsAgent = null;
if (proxyUrl) {
  httpsAgent = new HttpsProxyAgent(proxyUrl);
  console.log('🔐 Proxy enabled');
}

const clientOptions = {};
if (proxyUrl) clientOptions.ws = { agent: httpsAgent };
const client = new Client(clientOptions);

// -------------------- HARDCODED CONFIG (no env clutter) --------------------
const POLL_INTERVAL_SEC = 600;               // 10 minutes
const JOIN_RECOVERY_WINDOW_MIN = 30;         // only notify joins within last 30 min
const MAX_JOIN_AGE_MS = JOIN_RECOVERY_WINDOW_MIN * 60 * 1000;
const CHUNK_SIZE = 1000;                     // members per request
const PAGE_DELAY_MS = 2000;                  // delay between chunks

let pollingInterval = null;

// -------------------- UTILITIES --------------------
function randomDelay(min, max) {
  const ms = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function simulateActivity() {
  if (Math.random() > 0.7) {
    await client.user.setStatus('online');
    console.log('📍 Simulated activity');
  }
}

async function sendNotification(payload, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const config = proxyUrl ? { httpsAgent, timeout: 10000 } : { timeout: 10000 };
      await axios.post(process.env.NOTIFY_URL, payload, config);
      console.log(`✅ Notified: ${payload.username} joined ${payload.server}`);
      return;
    } catch (err) {
      if (i === maxRetries - 1) console.error(`❌ Notify failed:`, err.message);
      else await randomDelay(1000, 3000);
    }
  }
}

// -------------------- CHUNKED FETCH --------------------
async function fetchAllMembersInChunks(guild) {
  const members = new Map();
  let lastUserId = undefined;
  let page = 0;
  console.log(`[FETCH] ${guild.name} (~${guild.memberCount} members) – chunked ${CHUNK_SIZE}/page`);

  while (true) {
    try {
      const options = { limit: CHUNK_SIZE, withPresences: false };
      if (lastUserId) options.after = lastUserId;
      const chunk = await guild.members.fetch(options);
      if (chunk.size === 0) break;
      for (const [id, member] of chunk) members.set(id, member);
      lastUserId = chunk.lastKey();
      page++;
      console.log(`   Page ${page}: +${chunk.size} (total ${members.size})`);
      if (chunk.size < CHUNK_SIZE) break;
      await randomDelay(PAGE_DELAY_MS, PAGE_DELAY_MS + 500);
    } catch (err) {
      console.error(`[CHUNK ERROR] ${guild.name}:`, err.message);
      break;
    }
  }
  console.log(`[DONE] ${guild.name} → ${members.size} members`);
  return members;
}

// -------------------- POLL GUILD --------------------
async function pollGuild(guild) {
  console.log(`\n🔍 Polling ${guild.name}...`);
  const start = Date.now();
  const members = await fetchAllMembersInChunks(guild);
  if (members.size === 0) return;

  const guildKey = `guild:${guild.id}:members`;
  const now = Date.now();
  let newCount = 0, notified = 0;

  // Check membership in Redis (batch using pipeline)
  const pipeline = redis.pipeline();
  for (const [id] of members) pipeline.sismember(guildKey, id);
  const results = await pipeline.exec();

  let idx = 0;
  for (const [id, member] of members) {
    const isKnown = results[idx++][1];
    if (!isKnown) {
      newCount++;
      await redis.sadd(guildKey, id);
      const joinedAt = member.joinedAt ? member.joinedAt.getTime() : null;
      if (joinedAt && (now - joinedAt) <= MAX_JOIN_AGE_MS) {
        notified++;
        console.log(`🆕 ${member.user.tag} joined recently – notifying`);
        await sendNotification({
          server: guild.name,
          serverId: guild.id,
          userId: member.user.id,
          username: member.user.tag,
          joinedAt: member.joinedAt?.toISOString() || new Date().toISOString(),
          source: 'poll'
        });
      } else {
        console.log(`📦 Added old member: ${member.user.tag} (joined ${joinedAt ? Math.round((now-joinedAt)/60000) : '?'} min ago)`);
      }
    }
  }
  console.log(`[${guild.name}] Done in ${((Date.now()-start)/1000).toFixed(1)}s – ${members.size} members, ${newCount} new (${notified} notified)`);
}

// -------------------- POLLING SCHEDULER --------------------
async function runBackupPoll() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n🔄 Starting poll cycle (${guilds.length} guilds)...`);
  for (let i = 0; i < guilds.length; i++) {
    setTimeout(() => pollGuild(guilds[i]), i * 5000);
  }
  await simulateActivity();
}

function startPolling() {
  if (pollingInterval) clearInterval(pollingInterval);
  pollingInterval = setInterval(runBackupPoll, POLL_INTERVAL_SEC * 1000);
  console.log(`⏱️ Polling every ${POLL_INTERVAL_SEC}s (chunked, ${CHUNK_SIZE}/page, ${PAGE_DELAY_MS}ms delay)`);
}

// -------------------- DISCORD EVENTS --------------------
client.on('ready', async () => {
  console.log(`\n🤖 Selfbot: ${client.user.tag} | ${client.guilds.cache.size} servers`);
  startPolling();
});

client.on('guildDelete', (guild) => {
  redis.del(`guild:${guild.id}:members`).catch(console.error);
  console.log(`➖ Left: ${guild.name}`);
});

// -------------------- HEALTH SERVER --------------------
const app = express();
app.get('/', (req, res) => res.json({ status: 'running', guilds: client.guilds.cache.size }));
app.listen(3000, () => console.log('✅ Health server on :3000'));

// -------------------- ERROR HANDLING --------------------
process.on('unhandledRejection', (err) => {
  if (err.message?.toLowerCase().includes('invalid token')) console.error('🔴 TOKEN EXPIRED');
  else console.error('⚠️ Unhandled rejection:', err.message);
});
process.on('uncaughtException', (err) => {
  if (err.message?.toLowerCase().includes('invalid token')) console.error('🔴 TOKEN EXPIRED');
  else console.error('⚠️ Uncaught exception:', err.message);
});

client.login(process.env.USER_TOKEN);

const express = require('express');
const { HttpsProxyAgent } = require('https-proxy-agent');
require('dotenv').config();
const { Client } = require('discord.js-selfbot-v13');
const axios = require('axios');
const { Redis } = require('@upstash/redis');

// Redis
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// Proxy (optional)
const proxyUrl = process.env.PROXY_URL;
let httpsAgent = null;
if (proxyUrl) {
  httpsAgent = new HttpsProxyAgent(proxyUrl);
  console.log('🔐 Proxy enabled');
}
const clientOptions = {};
if (proxyUrl) clientOptions.ws = { agent: httpsAgent };
const client = new Client(clientOptions);

// Config
const POLL_INTERVAL_SEC = 600;
const CHUNK_SIZE = 1000;
const PAGE_DELAY_MS = 2000;
const GRACE_PERIOD_MS = 5 * 60 * 1000;

let pollingInterval = null;
let START_TIME = null;

// Persistent start time
async function getStartTime() {
  let start = await redis.get('global:start_time');
  if (!start) {
    start = Date.now();
    await redis.set('global:start_time', start);
    console.log(`🕒 First run – start time = ${new Date(start).toISOString()}`);
  } else {
    start = parseInt(start);
    console.log(`🕒 Existing start time = ${new Date(start).toISOString()}`);
  }
  return start;
}

// Utilities
function randomDelay(min, max) {
  return new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min));
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

// Chunked fetch
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

  if (members.size === 0 && guild.memberCount > 5000) {
    console.log(`[NOTE] ${guild.name} is a large server (${guild.memberCount}+ members) — Discord limits full member list fetch for regular user accounts. Real-time cache detection still active.`);
  }

  console.log(`[DONE] ${guild.name} → ${members.size} members`);
  return members;
}

// Poll guild
async function pollGuild(guild) {
  console.log(`\n🔍 Polling ${guild.name}...`);
  const pollStart = Date.now();
  const members = await fetchAllMembersInChunks(guild);
  if (members.size === 0) return;

  const guildKey = `guild:${guild.id}:members`;
  const isFirstScan = (await redis.exists(guildKey)) === 0;

  const memberIds = Array.from(members.keys());
  const pipeline = redis.pipeline();
  for (const id of memberIds) {
    pipeline.sismember(guildKey, id);
  }

  let results;
  try {
    results = await pipeline.exec();
  } catch (err) {
    console.error(`[PIPELINE ERROR] ${guild.name}:`, err.message);
    return;
  }

  const newIds = [];
  const notifications = [];
  let idx = 0;

  for (const [id, member] of members) {
    const isKnown = results[idx++];
    if (!isKnown) {
      newIds.push(id);
      const joinedAt = member.joinedAt?.getTime() ?? 0;
      const effectiveStart = START_TIME - GRACE_PERIOD_MS;

      if (!isFirstScan && joinedAt > effectiveStart) {
        console.log(`🆕 NEW member: ${member.user.tag} (joined ${new Date(joinedAt).toISOString()})`);
        notifications.push({
          server: guild.name,
          serverId: guild.id,
          userId: member.user.id,
          username: member.user.tag,
          joinedAt: new Date(joinedAt).toISOString(),
          source: 'poll'
        });
      } else {
        console.log(`📦 ${isFirstScan ? 'Baseline' : 'Old'} member: ${member.user.tag}`);
      }
    }
  }

  if (newIds.length) {
    await redis.sadd(guildKey, ...newIds);
  }
  await Promise.all(notifications.map(payload => sendNotification(payload)));

  const elapsed = ((Date.now() - pollStart) / 1000).toFixed(1);
  console.log(`[${guild.name}] ${isFirstScan ? 'BASELINE' : 'Incremental'} done in ${elapsed}s – ${members.size} total, ${newIds.length} new, ${notifications.length} notified`);
}

// Polling scheduler
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

// Discord events
client.on('ready', async () => {
  START_TIME = await getStartTime();
  console.log(`\n🤖 Selfbot: ${client.user.tag} | ${client.guilds.cache.size} servers`);
  startPolling();
});

client.on('guildDelete', (guild) => {
  redis.del(`guild:${guild.id}:members`).catch(console.error);
  console.log(`➖ Left: ${guild.name}`);
});

// shardDisconnect — detects token death
client.on('shardDisconnect', (event, shardId) => {
  console.error(`🔴 SHARD DISCONNECTED (shard ${shardId}): code=${event.code} reason=${event.reason}`);
  if (event.code === 4004) {
    console.error('================================');
    console.error('🔴 TOKEN EXPIRED – update USER_TOKEN immediately');
    console.error('================================');
  }
});

// Health + Stats endpoints
const app = express();

app.get('/', (req, res) => {
  res.json({
    status: 'running',
    guilds: client.guilds.cache.size,
    uptime: Math.floor(process.uptime()),
    timestamp: new Date().toISOString()
  });
});

app.get('/stats', async (req, res) => {
  try {
    const guilds = [...client.guilds.cache.values()];
    const stats = [];
    for (const guild of guilds) {
      const guildKey = `guild:${guild.id}:members`;
      const trackedCount = await redis.scard(guildKey);
      stats.push({
        name: guild.name,
        id: guild.id,
        memberCount: guild.memberCount,
        trackedInRedis: trackedCount || 0
      });
    }
    res.json({
      totalGuilds: guilds.length,
      guilds: stats,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('❌ /stats error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.listen(3000, () => console.log('✅ Health server on :3000'));

// Error handling
process.on('unhandledRejection', (err) => {
  const msg = err.message?.toLowerCase() || '';
  if (msg.includes('invalid token')) {
    console.error('================================');
    console.error('🔴 TOKEN EXPIRED – update USER_TOKEN');
    console.error('================================');
  } else {
    console.error('⚠️ Unhandled rejection:', err.message);
  }
});

process.on('uncaughtException', (err) => {
  const msg = err.message?.toLowerCase() || '';
  if (msg.includes('invalid token')) {
    console.error('================================');
    console.error('🔴 TOKEN EXPIRED – update USER_TOKEN');
    console.error('================================');
  } else {
    console.error('⚠️ Uncaught exception:', err.message);
  }
});

client.login(process.env.USER_TOKEN);

const express = require('express');
const { HttpsProxyAgent } = require('https-proxy-agent');
require('dotenv').config();
const { Client } = require('discord.js-selfbot-v13');
const axios = require('axios');

// -------------------- PROXY SETUP (WebSocket + HTTP) --------------------
const proxyUrl = process.env.PROXY_URL;
let httpsAgent = null;

if (proxyUrl) {
  httpsAgent = new HttpsProxyAgent(proxyUrl);
  console.log('🔐 Proxy enabled');
}

// WebSocket proxy for Discord connection
const clientOptions = {};
if (proxyUrl) {
  clientOptions.ws = { agent: httpsAgent };
}
const client = new Client(clientOptions);

// -------------------- STATE MANAGEMENT --------------------
const memberSnapshots = new Map();     // guildId -> Set of user IDs
const lastPollTime = new Map();        // guildId -> timestamp (ms)
const recentJoins = new Map();         // guildId -> Set of user IDs (detected via event)
let pollingInterval = null;

// Configuration
const POLL_INTERVAL_SEC = parseInt(process.env.POLL_INTERVAL_SEC) || 300;  // 5 min backup
const FAST_POLL_AFTER_JOIN_SEC = 60;   // Poll again 1 min after a join
const EVENT_DELAY_MS = 300;            // Human-like jitter

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
      console.log(`✅ Notification: ${payload.username} joined ${payload.server}`);
      return;
    } catch (err) {
      if (i === maxRetries - 1) {
        console.error(`❌ Notify failed after ${maxRetries} retries:`, err.message);
      } else {
        await randomDelay(1000, 3000);
        console.log(`🔄 Retry ${i+1}/${maxRetries}`);
      }
    }
  }
}

// -------------------- FAST PATH: Gateway Event --------------------
client.on('guildMemberAdd', async (member) => {
  const guild = member.guild;
  console.log(`⚡ INSTANT join: ${member.user.tag} in ${guild.name}`);

  // Update snapshot immediately
  let snapshot = memberSnapshots.get(guild.id);
  if (snapshot) {
    snapshot.add(member.user.id);
    memberSnapshots.set(guild.id, snapshot);
  } else {
    await initializeGuild(guild);
    snapshot = memberSnapshots.get(guild.id);
    if (snapshot) snapshot.add(member.user.id);
  }

  // Track for re‑poll
  if (!recentJoins.has(guild.id)) recentJoins.set(guild.id, new Set());
  recentJoins.get(guild.id).add(member.user.id);

  await randomDelay(EVENT_DELAY_MS, EVENT_DELAY_MS + 200);

  const payload = {
    server: guild.name,
    serverId: guild.id,
    username: member.user.tag,
    userId: member.user.id,
    joinedAt: member.joinedAt ? member.joinedAt.toISOString() : new Date().toISOString(),
    source: 'event'
  };
  await sendNotification(payload);

  // Fast re‑poll after join
  setTimeout(() => {
    if (client.guilds.cache.has(guild.id)) {
      console.log(`🔍 Fast re-poll for ${guild.name}`);
      pollGuild(guild, true);
    }
  }, FAST_POLL_AFTER_JOIN_SEC * 1000);
});

// -------------------- SAFE FETCH (with cache & timeout) --------------------
async function safeFetchMembers(guild, forceFresh = false) {
  const start = Date.now();
  const cacheSize = guild.members.cache.size;
  
  if (!forceFresh && cacheSize > 0 && (Date.now() - (lastPollTime.get(guild.id) || 0)) < 60000) {
    console.log(`[CACHE] ${guild.name} → ${cacheSize} members`);
    return guild.members.cache;
  }

  try {
    console.log(`[FETCH] ${guild.name} (~${guild.memberCount || '?'})`);
    const members = await guild.members.fetch({ time: 30000, withPresences: false });
    console.log(`[FETCH DONE] ${guild.name} in ${Date.now()-start}ms → ${members.size}`);
    return members;
  } catch (err) {
    console.error(`[FETCH FAIL] ${guild.name}:`, err.message);
    if (guild.members.cache.size > 0) {
      console.log(`[FALLBACK] Using stale cache for ${guild.name}`);
      return guild.members.cache;
    }
    throw err;
  }
}

// -------------------- INITIALIZE --------------------
async function initializeGuild(guild) {
  try {
    const members = await safeFetchMembers(guild, true);
    memberSnapshots.set(guild.id, new Set(members.map(m => m.user.id)));
    lastPollTime.set(guild.id, Date.now());
    console.log(`✅ Initialized ${guild.name} (${members.size} members)`);
  } catch (err) {
    console.error(`❌ Init failed ${guild.name}:`, err.message);
  }
}

// -------------------- BACKUP POLL (reconciliation) --------------------
async function pollGuild(guild, forceFresh = false) {
  try {
    const members = await safeFetchMembers(guild, forceFresh);
    const previousSnapshot = memberSnapshots.get(guild.id);
    const currentIds = new Set(members.map(m => m.user.id));
    
    if (!previousSnapshot) {
      memberSnapshots.set(guild.id, currentIds);
      lastPollTime.set(guild.id, Date.now());
      return;
    }

    // Detect missed joins
    const missedMembers = [];
    for (const id of currentIds) {
      if (!previousSnapshot.has(id)) {
        const member = members.get(id);
        if (member) missedMembers.push(member);
      }
    }

    recentJoins.delete(guild.id); // Clear after poll

    for (const member of missedMembers) {
      console.log(`🔄 POLL recovered missed join: ${member.user.tag} in ${guild.name}`);
      const payload = {
        server: guild.name,
        serverId: guild.id,
        username: member.user.tag,
        userId: member.user.id,
        joinedAt: member.joinedAt ? member.joinedAt.toISOString() : new Date().toISOString(),
        source: 'poll'
      };
      await sendNotification(payload);
    }

    memberSnapshots.set(guild.id, currentIds);
    lastPollTime.set(guild.id, Date.now());
    console.log(`[${guild.name}] Poll OK – ${currentIds.size} members${missedMembers.length ? ` (recovered ${missedMembers.length})` : ''}`);
  } catch (err) {
    console.error(`⚠️ Poll error ${guild.name}:`, err.message);
  }
}

// -------------------- BACKUP SCHEDULER --------------------
async function runBackupPoll() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n🔄 Backup poll (${guilds.length} guilds)...`);
  for (let i = 0; i < guilds.length; i++) {
    setTimeout(() => pollGuild(guilds[i], false), i * 2000);
  }
  await simulateActivity();
}

function startBackupPolling() {
  if (pollingInterval) clearInterval(pollingInterval);
  pollingInterval = setInterval(runBackupPoll, POLL_INTERVAL_SEC * 1000);
  console.log(`⏱️ Backup poll every ${POLL_INTERVAL_SEC}s (staggered)`);
}

// -------------------- EVENT HANDLERS --------------------
client.on('ready', async () => {
  console.log(`🤖 Selfbot: ${client.user.tag} | ${client.guilds.cache.size} servers`);
  const guilds = [...client.guilds.cache.values()];
  for (const guild of guilds) {
    await initializeGuild(guild);
    await randomDelay(300, 800);
  }
  startBackupPolling();
});

client.on('guildCreate', async (guild) => {
  console.log(`➕ Joined: ${guild.name}`);
  await initializeGuild(guild);
});

client.on('guildDelete', (guild) => {
  memberSnapshots.delete(guild.id);
  lastPollTime.delete(guild.id);
  recentJoins.delete(guild.id);
  console.log(`➖ Left: ${guild.name}`);
});

// -------------------- TOKEN EXPIRY DETECTION (ADDED) --------------------
client.on('shardDisconnect', (event, shardID) => {
  if (event?.code === 4004) {
    console.error(`🔴 TOKEN EXPIRED mid-session (shard ${shardID}) — Update USER_TOKEN in Render env vars!`);
  } else {
    console.warn(`⚠️ Shard ${shardID} disconnected with code ${event?.code}`);
  }
});

client.on('disconnect', (event) => {
  if (event?.code === 4004) {
    console.error('🔴 TOKEN EXPIRED — Update USER_TOKEN in Render env vars!');
  } else {
    console.warn('⚠️ Bot disconnected. Stopping polling...');
    if (pollingInterval) clearInterval(pollingInterval);
  }
});

// -------------------- KEEP-ALIVE & STATS --------------------
const app = express();
app.get('/', (req, res) => res.json({ status: 'running', guilds: client.guilds.cache.size }));
app.get('/stats', (req, res) => {
  const stats = {};
  for (const [id, snapshot] of memberSnapshots.entries()) {
    const guild = client.guilds.cache.get(id);
    stats[guild?.name || id] = { members: snapshot.size, lastPoll: lastPollTime.get(id) };
  }
  res.json(stats);
});
app.listen(3000, () => console.log('✅ Health server on :3000'));

// -------------------- ERROR HANDLING (UPDATED WITH TOKEN CHECK) --------------------
process.on('unhandledRejection', (err) => {
  if (err.message?.toLowerCase().includes('invalid token') || err.message?.toLowerCase().includes('an invalid token')) {
    console.error('🔴 TOKEN EXPIRED — Update USER_TOKEN in Render env vars!');
  } else {
    console.error('⚠️ Unhandled rejection:', err.message);
  }
});

process.on('uncaughtException', (err) => {
  if (err.message?.toLowerCase().includes('invalid token') || err.message?.toLowerCase().includes('an invalid token')) {
    console.error('🔴 TOKEN EXPIRED — Update USER_TOKEN in Render env vars!');
  } else {
    console.error('⚠️ Uncaught exception:', err.message);
  }
});

client.login(process.env.USER_TOKEN);

const express = require('express');
const { HttpsProxyAgent } = require('https-proxy-agent');
require('dotenv').config();
const { Client } = require('discord.js-selfbot-v13');
const axios = require('axios');
const { Redis } = require('@upstash/redis');

// -------------------- REDIS (persistent member storage) --------------------
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// -------------------- PROXY SETUP --------------------
const proxyUrl = process.env.PROXY_URL;
let httpsAgent = null;
if (proxyUrl) {
  httpsAgent = new HttpsProxyAgent(proxyUrl);
  console.log('🔐 Proxy enabled');
}

const clientOptions = {};
if (proxyUrl) clientOptions.ws = { agent: httpsAgent };
const client = new Client(clientOptions);

// -------------------- STATE (minimal in-memory) --------------------
let pollingInterval = null;
const lastPollTime = new Map();      // guildId -> timestamp (ms)

// Configuration
const POLL_INTERVAL_SEC = parseInt(process.env.POLL_INTERVAL_SEC) || 300;
const JOIN_RECOVERY_WINDOW_MIN = parseInt(process.env.JOIN_RECOVERY_WINDOW_MIN) || 30;
const MAX_JOIN_AGE_MS = JOIN_RECOVERY_WINDOW_MIN * 60 * 1000;

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

// -------------------- FETCH MEMBERS (no stale cache) --------------------
async function fetchMembers(guild, forceFresh = false) {
  const now = Date.now();
  if (!forceFresh && (now - (lastPollTime.get(guild.id) || 0)) < 60000) {
    console.log(`[CACHE] ${guild.name} → ${guild.members.cache.size} members`);
    return guild.members.cache;
  }
  try {
    console.log(`[FETCH] ${guild.name} (~${guild.memberCount || '?'})`);
    const members = await guild.members.fetch({ time: 30000, withPresences: false });
    console.log(`[FETCH DONE] ${guild.name} → ${members.size} members`);
    lastPollTime.set(guild.id, now);
    return members;
  } catch (err) {
    console.error(`[FETCH FAIL] ${guild.name}:`, err.message);
    throw err;
  }
}

// -------------------- POLL GUILD: incremental Redis storage + joinedAt check --------------------
async function pollGuild(guild) {
  try {
    const members = await fetchMembers(guild, false);
    const guildKey = `guild:${guild.id}:members`;
    const now = Date.now();

    const pipeline = redis.pipeline();
    for (const [id] of members) {
      pipeline.sismember(guildKey, id);
    }
    const results = await pipeline.exec();

    // Guard: handle potential Redis errors in pipeline
    const isKnownArray = results.map(r => {
      if (r[0]) {
        console.error(`❌ Redis pipeline error for ${guild.name}:`, r[0]);
        return 0; // assume not known on error
      }
      return r[1];
    });

    let newMemberCount = 0;
    let idx = 0;
    for (const [id, member] of members) {
      const isKnown = isKnownArray[idx++];
      if (!isKnown) {
        await redis.sadd(guildKey, id);
        newMemberCount++;

        let shouldNotify = false;
        const joinedAt = member.joinedAt ? new Date(member.joinedAt).getTime() : null;
        if (joinedAt !== null) {
          if (now - joinedAt <= MAX_JOIN_AGE_MS) {
            shouldNotify = true;
            console.log(`🆕 New member (recent join): ${member.user.tag} joined ${guild.name}`);
          } else {
            console.log(`📦 Old member discovered (joined ${Math.round((now-joinedAt)/60000)} min ago): ${member.user.tag} – not notifying`);
          }
        } else {
          console.log(`⏭️ Ignored member without joinedAt: ${member.user.tag} in ${guild.name}`);
        }

        if (shouldNotify) {
          const payload = {
            server: guild.name,
            serverId: guild.id,
            guildId: guild.id,
            userId: member.user.id,
            username: member.user.tag,
            joinedAt: member.joinedAt ? member.joinedAt.toISOString() : new Date().toISOString(),
            source: 'poll'
          };
          await sendNotification(payload);
        }
      }
    }
    console.log(`[${guild.name}] Poll OK – ${members.size} members, ${newMemberCount} new to DB`);
  } catch (err) {
    console.error(`⚠️ Poll error ${guild.name}:`, err.message);
  }
}

// -------------------- POLLING SCHEDULER --------------------
async function runBackupPoll() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n🔄 Backup poll (${guilds.length} guilds)...`);
  for (let i = 0; i < guilds.length; i++) {
    setTimeout(() => pollGuild(guilds[i]), i * 2000);
  }
  await simulateActivity();
}

function startBackupPolling() {
  if (pollingInterval) clearInterval(pollingInterval);
  pollingInterval = setInterval(runBackupPoll, POLL_INTERVAL_SEC * 1000);
  console.log(`⏱️ Backup poll every ${POLL_INTERVAL_SEC}s (staggered)`);
}

// -------------------- INITIALISE (no full fetch) --------------------
async function initialiseAllGuilds() {
  console.log('Initialising guilds – members will be discovered incrementally via Redis.');
  for (const guild of client.guilds.cache.values()) {
    console.log(`📁 ${guild.name} – ready for incremental discovery`);
  }
}

// -------------------- DISCORD EVENT HANDLERS --------------------
client.on('ready', async () => {
  console.log(`🤖 Selfbot: ${client.user.tag} | ${client.guilds.cache.size} servers`);
  await initialiseAllGuilds();
  startBackupPolling();
});

client.on('guildCreate', async (guild) => {
  console.log(`➕ Joined new server: ${guild.name}`);
});

client.on('guildDelete', (guild) => {
  redis.del(`guild:${guild.id}:members`).catch(console.error);
  console.log(`➖ Left: ${guild.name}`);
});

// -------------------- TOKEN EXPIRY DETECTION --------------------
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

// -------------------- HEALTH & STATS SERVER --------------------
const app = express();
app.get('/', (req, res) => res.json({ status: 'running', guilds: client.guilds.cache.size }));
app.get('/stats', async (req, res) => {
  const stats = {};
  for (const guild of client.guilds.cache.values()) {
    const count = await redis.scard(`guild:${guild.id}:members`);
    stats[guild.name] = {
      knownMembersInRedis: count,
      lastPoll: lastPollTime.get(guild.id),
      memberCount: guild.memberCount
    };
  }
  res.json(stats);
});
app.listen(3000, () => console.log('✅ Health server on :3000'));

// -------------------- ERROR HANDLING --------------------
process.on('unhandledRejection', (err) => {
  const msg = err.message?.toLowerCase() || '';
  if (msg.includes('invalid token') || msg.includes('an invalid token')) {
    console.error('🔴 TOKEN EXPIRED — Update USER_TOKEN in Render env vars!');
  } else {
    console.error('⚠️ Unhandled rejection:', err.message);
  }
});

process.on('uncaughtException', (err) => {
  const msg = err.message?.toLowerCase() || '';
  if (msg.includes('invalid token') || msg.includes('an invalid token')) {
    console.error('🔴 TOKEN EXPIRED — Update USER_TOKEN in Render env vars!');
  } else {
    console.error('⚠️ Uncaught exception:', err.message);
  }
});

// -------------------- START --------------------
client.login(process.env.USER_TOKEN);

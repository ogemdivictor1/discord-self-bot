const express = require('express');
const { HttpsProxyAgent } = require('https-proxy-agent');
require('dotenv').config();
const { Client } = require('discord.js-selfbot-v13');
const axios = require('axios');
const { Redis } = require('@upstash/redis');

// Redis Setup
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// Proxy Setup
const proxyUrl = process.env.PROXY_URL;
let httpsAgent = null;
if (proxyUrl) {
  httpsAgent = new HttpsProxyAgent(proxyUrl);
  console.log('🔐 Proxy enabled');
}
const clientOptions = {};
if (proxyUrl) clientOptions.ws = { agent: httpsAgent };
const client = new Client(clientOptions);

// Configuration
const POLL_INTERVAL_SEC = 600;
const GRACE_PERIOD_MS = 5 * 60 * 1000;

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
    try {
      await client.user.setStatus('online');
      console.log('📍 Simulated client activity heartbeat');
    } catch (err) {
      console.error('⚠️ Failed to simulate activity status:', err.message);
    }
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

// -------------------- OPTIMIZED ACTIVE MEMBER POLL --------------------
async function pollActiveMembersOnly(guild) {
  const pollStart = Date.now();
  try {
    // 1. Before fetch log
    console.log(`📡 Sending sidebar request to Discord for ${guild.name}...`);

    const activeMembers = await guild.members.fetch({
      query: '',
      limit: 250,
      withPresences: true,
      force: true
    });

    if (!activeMembers || activeMembers.size === 0) {
      console.log(`⚠️ No active members returned for ${guild.name}.`);
      return;
    }

    // 2. After fetch success log
    console.log(`✅ Discord responded with ${activeMembers.size} active members for ${guild.name}`);

    const guildKey = `guild:${guild.id}:members`;
    const effectiveStart = START_TIME - GRACE_PERIOD_MS;

    const memberArray = Array.from(activeMembers.values());
    const pipeline = redis.pipeline();

    for (const member of memberArray) {
      pipeline.sismember(guildKey, member.id);
    }

    // 3. During Redis pipeline log
    console.log(`🔍 Checking ${memberArray.length} members against Redis...`);
    const redisResults = await pipeline.exec();

    const newIdsToTrack = [];
    const notifications = [];

    for (let i = 0; i < memberArray.length; i++) {
      const member = memberArray[i];
      const isKnown = redisResults[i];
      const joinedAt = member.joinedAt?.getTime() ?? 0;

      if (joinedAt > effectiveStart && !isKnown) {
        newIdsToTrack.push(member.id);
        console.log(`🆕 Active Join: ${member.user.tag} (Joined ${new Date(joinedAt).toISOString()})`);

        notifications.push({
          server: guild.name,
          serverId: guild.id,
          userId: member.user.id,
          username: member.user.tag,
          joinedAt: new Date(joinedAt).toISOString(),
          source: 'active_sidebar'
        });
      }
    }

    // 4. After Redis results log
    console.log(`📊 Redis check complete — ${newIdsToTrack.length} new, ${memberArray.length - newIdsToTrack.length} already known`);

    if (newIdsToTrack.length > 0) {
      // 5. Before saving to Redis log
      console.log(`💾 Saving ${newIdsToTrack.length} new member IDs to Redis...`);
      await redis.sadd(guildKey, ...newIdsToTrack);
    }

    if (notifications.length > 0) {
      // 6. Before sending notifications log
      console.log(`📨 Sending ${notifications.length} notifications to CYPHER XXD...`);
      await Promise.all(notifications.map(payload => sendNotification(payload)));
    }

    const elapsed = ((Date.now() - pollStart) / 1000).toFixed(1);
    console.log(`[${guild.name}] Checked ${activeMembers.size} members in ${elapsed}s. Sent ${notifications.length} alerts.`);

  } catch (err) {
    console.error(`❌ Active fetch failed for ${guild.name}:`, err.message);
  }
}

// -------------------- POLL SCHEDULER --------------------
async function runActivePollCycle() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n🔄 Starting active member scan across ${guilds.length} servers...`);

  for (const guild of guilds) {
    await pollActiveMembersOnly(guild);
    await randomDelay(4000, 7000);
  }

  await simulateActivity();
  console.log(`\n🏁 Scan cycle complete. Resting for ${POLL_INTERVAL_SEC} seconds.`);
  setTimeout(runActivePollCycle, POLL_INTERVAL_SEC * 1000);
}

// -------------------- DISCORD EVENTS --------------------
client.on('ready', async () => {
  START_TIME = await getStartTime();
  console.log(`\n🤖 Selfbot Active Monitor: ${client.user.tag} | Watching ${client.guilds.cache.size} servers`);
  runActivePollCycle();
});

client.on('guildDelete', (guild) => {
  redis.del(`guild:${guild.id}:members`).catch(console.error);
  console.log(`➖ Left: ${guild.name}`);
});

// -------------------- TOKEN EXPIRY --------------------
client.on('shardDisconnect', (event, shardId) => {
  if (event?.code === 4004) {
    console.error('================================================');
    console.error(`🔴 TOKEN EXPIRED (shard ${shardId}) — Update USER_TOKEN!`);
    console.error('================================================');
  } else {
    console.warn(`⚠️ Shard ${shardId} disconnected: code=${event?.code || 'Unknown'}`);
  }
});

// -------------------- HEALTH + STATS --------------------
const app = express();

app.get('/', (req, res) => {
  res.json({
    status: 'running',
    mode: 'active_sidebar_only',
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

app.listen(3000, () => console.log('✅ Health server listening on port 3000'));

// -------------------- GLOBAL EXCEPTION SAFETY --------------------
function handleFatalError(err) {
  const msg = err.message?.toLowerCase() || '';
  if (msg.includes('invalid token') || msg.includes('401: unauthorized')) {
    console.error('================================================');
    console.error('🔴 CRITICAL TOKEN EXPIRED — Update USER_TOKEN immediately.');
    console.error('================================================');
  } else {
    console.error('⚠️ Runtime Exception intercepted:', err.message);
  }
}

process.on('unhandledRejection', handleFatalError);
process.on('uncaughtException', handleFatalError);

client.login(process.env.USER_TOKEN);

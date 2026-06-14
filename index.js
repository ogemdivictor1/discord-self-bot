const express = require('express');
const { HttpsProxyAgent } = require('https-proxy-agent');
require('dotenv').config();
const { Client } = require('discord.js-selfbot-v13');
const axios = require('axios');
const { Redis } = require('@upstash/redis');

// 1. DATABASE SETUP
console.log('🔄 [System Initialization] Connecting to Upstash Redis...');
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// 2. BOT CONFIG
const proxyUrl = process.env.PROXY_URL;
let httpsAgent = proxyUrl ? new HttpsProxyAgent(proxyUrl) : null;
const clientOptions = proxyUrl ? { ws: { agent: httpsAgent } } : {};
const client = new Client(clientOptions);

const POLL_INTERVAL_SEC = 90; 
const GRACE_PERIOD_MS = 5 * 60 * 1000; 
let START_TIME = null;

// Load baseline startup time
async function getStartTime() {
  console.log('⏳ [Storage Sync] Requesting global baseline START_TIME from Redis...');
  try {
    let start = await redis.get('global:start_time');
    if (!start) {
      start = Date.now();
      await redis.set('global:start_time', start);
    } else {
      start = parseInt(start);
    }
    return start;
  } catch (err) {
    return Date.now();
  }
}

// Global alert dispatcher
async function sendNotification(payload) {
  try {
    const config = proxyUrl ? { httpsAgent, timeout: 10000 } : { timeout: 10000 };
    await axios.post(process.env.NOTIFY_URL, payload, config);
    console.log(`✅ [Webhook Dispatcher] Delivery confirmed for user [${payload.username}] in [${payload.server}] via [${payload.source}]`);
  } catch (err) {
    console.error(`❌ [Webhook Dispatcher Error] Delivery failed for user [${payload.username}]:`, err.message);
  }
}

// ────────────────────────────────────────────────────────
// 🎣 THE CENTRAL CORE: PROCESS AND FILTER DISCOVERED USERS
// ────────────────────────────────────────────────────────
async function processDiscoveredMembers(guild, memberMap, sourceLabel) {
  if (!guild || memberMap.size === 0) return;

  const guildKey = `guild:${guild.id}:members`;
  const effectiveStart = START_TIME - GRACE_PERIOD_MS;
  const memberArray = Array.from(memberMap.values());

  const pipeline = redis.pipeline();
  for (const member of memberArray) {
    if (member && member.id) pipeline.sismember(guildKey, member.id);
  }
  
  let redisResults;
  try {
    redisResults = await pipeline.exec();
  } catch (err) {
    console.error(`❌ [Central Processor Error] Database pipeline failed for [${guild.name}]:`, err.message);
    return;
  }

  const newIdsToTrack = [];
  const notifications = [];

  for (let i = 0; i < memberArray.length; i++) {
    const member = memberArray[i];
    if (!member || !member.user) continue;
    
    const isKnown = redisResults[i]; 
    const joinedAt = member.joinedAt?.getTime() ?? 0;

    if (!isKnown) {
      newIdsToTrack.push(member.id);

      if (joinedAt > effectiveStart) {
        console.log(`🎯 [TIMESTAMP ALERT DETECTED] Hook matched via [${sourceLabel}] inside [${guild.name}]: [${member.user.tag}]`);
        notifications.push({
          server: guild.name,
          serverId: guild.id,
          userId: member.user.id,
          username: member.user.tag,
          joinedAt: new Date(joinedAt).toISOString(),
          source: sourceLabel
        });
      }
    }
  }

  if (newIdsToTrack.length > 0) {
    await redis.sadd(guildKey, ...newIdsToTrack).catch(() => {});
  }

  if (notifications.length > 0) {
    await Promise.all(notifications.map(payload => sendNotification(payload)));
  }
}

// ────────────────────────────────────────────────────────
// ⚙️ ENGINE 1: SMART ACTIVE NETS (MULTI-SEED RAPID QUERY ENGINE)
// ────────────────────────────────────────────────────────
async function pollGuildActiveEngine(guild) {
  console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`📡 [Active Loop Engine] Polling Target: [${guild.name}] (Roster Capacity: ${guild.memberCount})`);

  try {
    const activeGathered = new Map();
    const baseTargets = ['2026', '2025', 'sol', 'eth', 'dev', 'the', 'a', 'e', 's', 'i', 'o'];
    const growthTargets = ['crypto', 'nft', 'trade', 'alpha', 'call', 'vc', 'lfg', 'he'];

    if (guild.memberCount < 2000) {
      console.log(`🧹 [Active Engine -> Strategy A] Small group. Running multi-seed text scans...`);
      const randomSeeds = growthTargets.sort(() => 0.5 - Math.random()).slice(0, 3);
      
      for (const seed of randomSeeds) {
        const fetchSlice = await guild.members.fetch({ query: seed, limit: 100, time: 8000, withPresences: false }).catch(() => null);
        if (fetchSlice) fetchSlice.forEach(m => activeGathered.set(m.id, m));
        await new Promise(r => setTimeout(r, 800));
      }
    } else {
      console.log(`⚠️ [Active Engine -> Strategy B] Large server. Deploying dual keyword search pass...`);
      const kw1 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      const slice1 = await guild.members.fetch({ query: kw1, limit: 50, time: 10000, withPresences: false }).catch(() => null);
      if (slice1) slice1.forEach(m => activeGathered.set(m.id, m));

      await new Promise(r => setTimeout(r, 2000));

      let kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      while (kw2 === kw1) kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];

      const slice2 = await guild.members.fetch({ query: kw2, limit: 50, time: 10000, withPresences: false }).catch(() => null);
      if (slice2) slice2.forEach(m => activeGathered.set(m.id, m));
    }

    const strategyLabel = guild.memberCount < 2000 ? 'small_group_multi_seed' : 'large_group_dual_query';
    await processDiscoveredMembers(guild, activeGathered, strategyLabel);
    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

  } catch (err) {
    console.error(`❌ [Active Loop Engine Error] Faulted on ${guild.name}:`, err.message);
  }
}

async function startActiveLoopCycle() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n============================================================`);
  console.log(`🔄 [Loop Scheduler] Starting loop cycle across ${guilds.length} total guilds...`);
  console.log(`============================================================`);

  for (const guild of guilds) {
    await pollGuildActiveEngine(guild);
    await new Promise(r => setTimeout(r, Math.floor(Math.random() * 3000) + 4000));
  }

  setTimeout(startActiveLoopCycle, POLL_INTERVAL_SEC * 1000);
}

// ────────────────────────────────────────────────────────
// 🎙️ ENGINE 2: PASSIVE NETS (LIVE STREAMING ENGINE)
// ────────────────────────────────────────────────────────

// NET 1: Native Join Gateway Handler (Safe Fallback)
client.on('guildMemberAdd', async (member) => {
  if (member.user.bot) return;
  const standardMap = new Map([[member.id, member]]);
  await processDiscoveredMembers(member.guild, standardMap, 'guild_member_add');
});

// NET 2: Native Metadata Update Hooks
client.on('guildMemberUpdate', async (oldMember, newMember) => {
  if (newMember.user.bot) return;
  const standardMap = new Map([[newMember.id, newMember]]);
  await processDiscoveredMembers(newMember.guild, standardMap, 'guild_member_update');
});

// NET 3 & NET 4: Live text chat logs + Mentions/Announcements
client.on('messageCreate', async (message) => {
  if (!message.guild || message.author.bot) return;

  if (message.member) {
    const standardMap = new Map([[message.member.id, message.member]]);
    await processDiscoveredMembers(message.guild, standardMap, 'live_message_stream');
  }

  if (message.mentions.members.size > 0) {
    await processDiscoveredMembers(message.guild, message.mentions.members, 'welcome_system_hook');
  }
});

// NET 5: Chat channel typing tracking
client.on('typingStart', async (channel, user) => {
  if (!channel.guild || user.bot) return;
  const member = channel.guild.members.cache.get(user.id);
  if (member) {
    const typingMap = new Map([[member.id, member]]);
    await processDiscoveredMembers(channel.guild, typingMap, 'live_typing_indicator');
  }
});

// NET 6: Client status modifications (online shifts)
client.on('presenceUpdate', async (oldPres, newPres) => {
  if (!newPres || !newPres.guild || !newPres.member || newPres.user.bot) return;
  const presenceMap = new Map([[newPres.member.id, newPres.member]]);
  await processDiscoveredMembers(newPres.guild, presenceMap, 'live_presence_shift');
});

// NET 7: Emoji/Reaction click verification hooks
client.on('messageReactionAdd', async (reaction, user) => {
  if (!reaction.message.guild || user.bot) return;
  const member = await reaction.message.guild.members.fetch(user.id).catch(() => null);
  if (member) {
    const reactionMap = new Map([[member.id, member]]);
    await processDiscoveredMembers(reaction.message.guild, reactionMap, 'live_reaction_hook');
  }
});

// NET 8: Dynamic sub-thread updates
client.on('threadMembersUpdate', async (oldMembers, newMembers) => {
  const sampleMember = newMembers.first();
  if (!sampleMember || !sampleMember.guild) return;
  
  const threadMap = new Map();
  newMembers.forEach(m => {
    if (m.guildMember && !m.guildMember.user.bot) threadMap.set(m.guildMember.id, m.guildMember);
  });

  if (threadMap.size > 0) await processDiscoveredMembers(sampleMember.guild, threadMap, 'thread_activity_hook');
});

// NET 9 & NET 18: Voice room activity + Micro state mutations
client.on('voiceStateUpdate', async (oldState, newState) => {
  if (!newState.guild || !newState.member || newState.member.user.bot) return;
  
  // Catches joins (Net 9) or streaming/muting anomalies (Net 18)
  if (newState.channelId) {
    const isMicroShift = oldState.channelId === newState.channelId;
    const sourceLabel = isMicroShift ? 'voice_micro_mutation_hook' : 'voice_channel_state_hook';
    const voiceMap = new Map([[newState.member.id, newState.member]]);
    await processDiscoveredMembers(newState.guild, voiceMap, sourceLabel);
  }
});

// NET 10: General user profile updates (Avatar updates, custom bio text)
client.on('userUpdate', async (oldUser, newUser) => {
  if (newUser.bot) return;
  client.guilds.cache.forEach((guild) => {
    const member = guild.members.cache.get(newUser.id);
    if (member) {
      const profileMap = new Map([[member.id, member]]);
      processDiscoveredMembers(guild, profileMap, 'client_profile_sync_hook').catch(() => {});
    }
  });
});

// NET 11, NET 12 & NET 13: Raw Packet Exception Decoders
client.on('raw', async (packet) => {
  try {
    if (packet.t === 'GUILD_MEMBERS_CHUNK') {
      const { guild_id, members } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (!guild || !members) return;

      const chunkMap = new Map();
      for (const data of members) {
        if (data.user && !data.user.bot) {
          const m = await guild.members.fetch(data.user.id).catch(() => null);
          if (m) chunkMap.set(m.id, m);
        }
      }
      if (chunkMap.size > 0) await processDiscoveredMembers(guild, chunkMap, 'raw_gateway_chunk_hook');
    }

    if (packet.t === 'GUILD_MEMBER_ADD') {
      const { guild_id, user } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (guild && user && !user.bot) {
        const m = await guild.members.fetch(user.id).catch(() => null);
        if (m) await processDiscoveredMembers(guild, new Map([[m.id, m]]), 'raw_guild_member_add');
      }
    }

    if (packet.t === 'GUILD_MEMBER_UPDATE') {
      const { guild_id, user } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (guild && user && !user.bot) {
        const m = await guild.members.fetch(user.id).catch(() => null);
        if (m) await processDiscoveredMembers(guild, new Map([[m.id, m]]), 'raw_guild_member_update');
      }
    }
  } catch (err) {}
});

// NET 14: Automated workspace add loops
client.on('guildCreate', (guild) => {
  console.log(`📥 [Workspace Extension -> Net 14] Joined pool: ${guild.name}. Spawning micro-checks...`);
  setTimeout(() => pollGuildActiveEngine(guild).catch(() => {}), 5000);
});

// NET 15: Pin-Board Sneak Hook
client.on('channelPinsUpdate', async (channel) => {
  if (!channel.guild) return;
  console.log(`🎙️ [Passive Gateway -> Net 15] Pin modification detected inside [#${channel.name}] in [${channel.guild.name}]. Scraping lane...`);
  try {
    const recentMessages = await channel.messages.fetch({ limit: 10 }).catch(() => null);
    if (!recentMessages) return;

    const collectedMap = new Map();
    recentMessages.forEach(msg => {
      if (msg.member && !msg.author.bot) collectedMap.set(msg.member.id, msg.member);
    });

    if (collectedMap.size > 0) {
      await processDiscoveredMembers(channel.guild, collectedMap, 'channel_pin_proximity_hook');
    }
  } catch (err) {}
});

// NET 16: Thread Initialization Watcher
client.on('threadCreate', async (thread) => {
  if (!thread.guild) return;
  console.log(`🎙️ [Passive Gateway -> Net 16] Live sub-thread initialized inside [${thread.guild.name}]`);
  try {
    // Fetch the thread starter profile
    const ownerId = thread.ownerId;
    if (!ownerId) return;
    const member = await thread.guild.members.fetch(ownerId).catch(() => null);
    if (member && !member.user.bot) {
      await processDiscoveredMembers(thread.guild, new Map([[member.id, member]]), 'thread_initiation_hook');
    }
  } catch (err) {}
});

// NET 17: Application/Slash Command Verification Hook
client.on('interactionCreate', async (interaction) => {
  if (!interaction.guild || !interaction.member || interaction.user.bot) return;
  console.log(`🎙️ [Passive Gateway -> Net 17] Application/Slash component interaction from [${interaction.user.tag}] inside [${interaction.guild.name}]`);
  const interactionMap = new Map([[interaction.member.id, interaction.member]]);
  await processDiscoveredMembers(interaction.guild, interactionMap, 'client_interaction_hook');
});

// ────────────────────────────────────────────────────────
// 🚨 MANAGEMENT EVENTS
// ────────────────────────────────────────────────────────
client.on('ready', async () => {
  console.log(`\n============================================================`);
  console.log(`🤖 FISHTANK ONLINE AND ACTIVE ON -> [${client.user.tag}]`);
  console.log(`📊 Infrastructure Coverage Matrix: 18 Distinct Nets Operating Across ${client.guilds.cache.size} Servers.`);
  console.log(`============================================================`);
  START_TIME = await getStartTime();
  startActiveLoopCycle(); 
});

client.on('guildDelete', (guild) => {
  redis.del(`guild:${guild.id}:members`).catch(() => {});
});

client.on('shardDisconnect', (event, shardId) => {
  if (event?.code === 4004) {
    console.error('============================================================');
    console.error(`🔴 CRITICAL AUTH SESSION TERMINATED (shard ${shardId}) — RE-CHECK TOKEN!`);
    console.error('============================================================');
  }
});

// ────────────────────────────────────────────────────────
// 🌐 LIVE HOST HEALTH CHECK & TELEMETRY API
// ────────────────────────────────────────────────────────
const app = express();
app.get('/', (req, res) => res.json({ status: 'always_fishing', nets: 18, targets: client.guilds.cache.size }));
app.get('/stats', async (req, res) => {
  try {
    const guilds = [...client.guilds.cache.values()];
    const stats = [];
    for (const guild of guilds) {
      const trackedCount = await redis.scard(`guild:${guild.id}:members`);
      stats.push({ name: guild.name, id: guild.id, memberCount: guild.memberCount, trackedInRedis: trackedCount || 0 });
    }
    res.json({ totalGuilds: guilds.length, guilds: stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(3000, () => console.log('✅ [HTTP Server] Diagnostic telemetry channel open via port 3000'));

process.on('unhandledRejection', (err) => console.error('⚠️ [Engine Crash Guard] Intercepted unhandled rejection:', err.message));
process.on('uncaughtException', (err) => console.error('⚠️ [Engine Crash Guard] Intercepted global exception:', err.message));

client.login(process.env.USER_TOKEN);

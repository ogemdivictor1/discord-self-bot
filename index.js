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

// Global alert dispatcher (Webhook Dispatcher)
async function sendNotification(payload) {
  try {
    const config = proxyUrl ? { httpsAgent, timeout: 10000 } : { timeout: 10000 };
    await axios.post(process.env.NOTIFY_URL, payload, config);
    console.log(`🚀 [WEBHOOK DISPATCHER] -> SUCCESS: Alert delivered for [${payload.username}] caught by [${payload.source}] in server [${payload.server}]`);
  } catch (err) {
    console.error(`❌ [WEBHOOK DISPATCHER ERROR] -> Delivery failed for user [${payload.username}]:`, err.message);
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
    console.error(`❌ [CENTRAL PROCESSOR ERROR] Database pipeline failed for [${guild.name}]:`, err.message);
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

      // The Final Gatekeeper: joinedAt timestamp verification
      if (joinedAt > effectiveStart) {
        console.log(`🎯 [TIMESTAMP HIT] -> Identified brand new user [${member.user.tag}] via [${sourceLabel}] inside [${guild.name}]`);
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
// ⚙️ ENGINE 1: SMART ACTIVE NETS (ROUTINE SEARCH SCRAPER)
// ────────────────────────────────────────────────────────
async function pollGuildActiveEngine(guild) {
  console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`📡 [ACTIVE LOOP ENGINE] -> Indexing target: [${guild.name}] (Server Capacity: ${guild.memberCount})`);

  try {
    const activeGathered = new Map();
    const baseTargets = ['2026', '2025', 'sol', 'eth', 'dev', 'the', 'a', 'e', 's', 'i', 'o'];
    const growthTargets = ['crypto', 'nft', 'trade', 'alpha', 'call', 'vc', 'lfg', 'he'];

    // ACTIVE STRATEGY A: Small Server Multi-Seed Search Sweep
    if (guild.memberCount < 2000) {
      console.log(`🧹 [ACTIVE STRATEGY A] -> Group size under 2000. Initiating rapid multi-seed fishing lines...`);
      const randomSeeds = growthTargets.sort(() => 0.5 - Math.random()).slice(0, 3);
      
      for (const seed of randomSeeds) {
        console.log(`⏳ [ACTIVE ENGINE] -> Casting keyword search: ['${seed}']`);
        const fetchSlice = await guild.members.fetch({ query: seed, limit: 100, time: 8000, withPresences: false }).catch(() => null);
        if (fetchSlice && fetchSlice.size > 0) {
          console.log(`✅ [ACTIVE ENGINE SUCCESS] -> Found ${fetchSlice.size} profiles via keyword ['${seed}']`);
          fetchSlice.forEach(m => activeGathered.set(m.id, m));
        }
        await new Promise(r => setTimeout(r, 800));
      }

    // ACTIVE STRATEGY B: Large Server Dual Keyword Search Sweep
    } else {
      console.log(`⚠️ [ACTIVE STRATEGY B] -> Server size exceeds safety threshold. Executing dual cross-section queries...`);

      const kw1 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      console.log(`⏳ [ACTIVE ENGINE] -> Casting primary pass query: ['${kw1}'] (Limit: 50)`);
      const slice1 = await guild.members.fetch({ query: kw1, limit: 50, time: 10000, withPresences: false }).catch(() => null);
      if (slice1 && slice1.size > 0) {
        console.log(`✅ [ACTIVE ENGINE SUCCESS] -> Primary pass ['${kw1}'] caught ${slice1.size} unique targets.`);
        slice1.forEach(m => activeGathered.set(m.id, m));
      }

      await new Promise(r => setTimeout(r, 2000));

      let kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      while (kw2 === kw1) kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];

      console.log(`⏳ [ACTIVE ENGINE] -> Casting secondary pass query: ['${kw2}'] (Limit: 50)`);
      const slice2 = await guild.members.fetch({ query: kw2, limit: 50, time: 10000, withPresences: false }).catch(() => null);
      if (slice2 && slice2.size > 0) {
        console.log(`✅ [ACTIVE ENGINE SUCCESS] -> Secondary pass ['${kw2}'] caught ${slice2.size} unique targets.`);
        slice2.forEach(m => activeGathered.set(m.id, m));
      }
    }

    // NET 20: Audit Log Sneak Peek (Triggered during the server's loop cycle)
    try {
      console.log(`🕵️‍♂️ [NET 20: Audit Log Sneak] -> Poking backend audit tables for hidden join activity footprints...`);
      const auditLogs = await guild.fetchAuditLogs({ limit: 5 }).catch(() => null);
      if (auditLogs && auditLogs.entries.size > 0) {
        const auditMap = new Map();
        for (const entry of auditLogs.entries.values()) {
          if (entry.targetType === 'USER' && entry.target && !entry.target.bot) {
            const auditMember = await guild.members.fetch(entry.target.id).catch(() => null);
            if (auditMember) auditMap.set(auditMember.id, auditMember);
          }
        }
        if (auditMap.size > 0) {
          console.log(`✅ [NET 20 CATCH] -> Extracted ${auditMap.size} unique user structures directly out of internal audit registries.`);
          await processDiscoveredMembers(guild, auditMap, 'NET_20_AUDIT_LOG_SNEAK');
        }
      }
    } catch (e) {}

    const strategyLabel = guild.memberCount < 2000 ? 'ACTIVE_NET_STRATEGY_A_LOOP' : 'ACTIVE_NET_STRATEGY_B_LOOP';
    await processDiscoveredMembers(guild, activeGathered, strategyLabel);
    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

  } catch (err) {
    console.error(`❌ [ACTIVE LOOP ENGINE CRASHED] Failed execution slice for ${guild.name}:`, err.message);
  }
}

async function startActiveLoopCycle() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n============================================================`);
  console.log(`🔄 [Loop Scheduler] Dispensing scanning lines across ${guilds.length} total guilds...`);
  console.log(`============================================================`);

  for (const guild of guilds) {
    await pollGuildActiveEngine(guild);
    
    // Human Simulation Trait: Enforces realistic navigation delays between workspace swaps
    const humanDelay = Math.floor(Math.random() * (14000 - 8000 + 1) + 8000);
    console.log(`💤 [Human Simulator Delay] Pausing for ${(humanDelay / 1000).toFixed(1)}s before checking next guild panel...`);
    await new Promise(r => setTimeout(r, humanDelay));
  }

  // Jittered Polling: Randomized timer interval variations to bypass cloud firewall detection loops
  const jitteredIntervalSec = Math.floor(Math.random() * (115 - 75 + 1) + 75);
  console.log(`🏁 [Loop Scheduler] Run complete. Sleeping loop for ${jitteredIntervalSec}s via variable clock jitter.`);
  setTimeout(startActiveLoopCycle, jitteredIntervalSec * 1000);
}

// ────────────────────────────────────────────────────────
// 🎙️ ENGINE 2: PASSIVE NETS (LIVE GATEWAY PACKET LISTENERS)
// ────────────────────────────────────────────────────────

// NET 1 & NET 19: Native Joins + Premium Client Status Updates
client.on('guildMemberAdd', async (member) => {
  if (member.user.bot) return;
  console.log(`🎙️ [NET 1: Front Door Welcomer] -> Target [${member.user.tag}] tripped entrance gateway in [${member.guild.name}]`);
  await processDiscoveredMembers(member.guild, new Map([[member.id, member]]), 'NET_1_GUILD_MEMBER_ADD');
});

client.on('guildMemberUpdate', async (oldMember, newMember) => {
  if (newMember.user.bot) return;
  
  // NET 19: Premium Hidden Channel Booster Net
  const oldRoles = oldMember?.roles?.cache ?? new Map();
  const newRoles = newMember.roles.cache;
  if (newRoles.size > oldRoles.size) {
    console.log(`🎙️ [NET 19: Hidden Channels Welcome] -> Role mutation shift isolated on [${newMember.user.tag}] in [${newMember.guild.name}]`);
    await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_19_PREMIUM_ROLE_BOOST');
    return;
  }
  
  // NET 2: Standard Roster Profile Update Catch
  console.log(`🎙️ [NET 2: Profile Changer] -> Data field alteration caught for [${newMember.user.tag}] inside [${newMember.guild.name}]`);
  await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_2_GUILD_MEMBER_UPDATE');
});

// NET 3, NET 4, & NET 22: Live Chat Text Streams + Verification Mention Scraping
client.on('messageCreate', async (message) => {
  if (!message.guild) return;

  // NET 22: Webhook & Bot Mention Mirror Scraper
  if (message.author.bot || message.webhookId) {
    const rawContent = message.content || '';
    const embedContent = message.embeds?.map(e => `${e.title || ''} ${e.description || ''}`).join(' ') || '';
    const consolidatedText = `${rawContent} ${embedContent}`;
    
    // Check if the automated robot logs mention user IDs or tags
    const idRegex = /\b(\d{17,19})\b/g;
    let matches = [...consolidatedText.matchAll(idRegex)].map(m => m[1]);
    
    if (matches.length > 0) {
      const parsedMap = new Map();
      for (const matchedId of matches) {
        const parsedMember = await message.guild.members.fetch(matchedId).catch(() => null);
        if (parsedMember && !parsedMember.user.bot) parsedMap.set(parsedMember.id, parsedMember);
      }
      if (parsedMap.size > 0) {
        console.log(`🎙️ [NET 22: Bot Mention Scraper] -> Extracted ${parsedMap.size} user links out of system text logs inside [${message.guild.name}]`);
        await processDiscoveredMembers(message.guild, parsedMap, 'NET_22_BOT_MENTION_SCRAPER');
      }
    }
    return;
  }

  // NET 3: Human Text Stream Catch
  if (message.member) {
    console.log(`🎙️ [NET 3: Active Chatter] -> Chat frame dropped by [${message.author.tag}] in [#${message.channel.name}] inside [${message.guild.name}]`);
    await processDiscoveredMembers(message.guild, new Map([[message.member.id, message.member]]), 'NET_3_LIVE_MESSAGE');
  }

  // NET 4: Welcome Message System Core Mention Check
  if (message.mentions.members.size > 0) {
    console.log(`🎙️ [NET 4: Welcome Mention] -> Identity mentions broadcasted via chat lines inside [${message.guild.name}]`);
    await processDiscoveredMembers(message.guild, message.mentions.members, 'NET_4_WELCOME_SYSTEM_MENTIONS');
  }
});

// NET 5: Chat Box Action Tracker
client.on('typingStart', async (channel, user) => {
  if (!channel.guild || user.bot) return;
  const member = channel.guild.members.cache.get(user.id);
  if (member) {
    console.log(`🎙️ [NET 5: Typing Shadow] -> Input indicator tripped by [${user.tag}] inside channel [#${channel.name}] in [${channel.guild.name}]`);
    await processDiscoveredMembers(channel.guild, new Map([[member.id, member]]), 'NET_5_TYPING_INDICATOR');
  }
});

// NET 6 & NET 21: Client Status Mutations + Rich Presence Application Scrapers
client.on('presenceUpdate', async (oldPres, newPres) => {
  if (!newPres || !newPres.guild || !newPres.member || newPres.user.bot) return;
  
  // NET 21: Rich Presence Activity Tracker
  const oldActivities = oldPres?.activities?.map(a => a.name).join(',') || '';
  const newActivities = newPres.activities?.map(a => a.name).join(',') || '';
  if (oldActivities !== newActivities) {
    console.log(`🎙️ [NET 21: Rich Presence Scraper] -> External client application swap tracked for [${newPres.user.tag}] in [${newPres.guild.name}]`);
    await processDiscoveredMembers(newPres.guild, new Map([[newPres.member.id, newPres.member]]), 'NET_21_RICH_PRESENCE_SHIFT');
    return;
  }

  // NET 6: Standard Green Light Online Watcher
  console.log(`🎙️ [NET 6: Green Light Watcher] -> Connectivity status frame shift logged for [${newPres.user.tag}] inside [${newPres.guild.name}]`);
  await processDiscoveredMembers(newPres.guild, new Map([[newPres.member.id, newPres.member]]), 'NET_6_PRESENCE_STATUS_SHIFT');
});

// NET 7: Emoji Reactions Tracker
client.on('messageReactionAdd', async (reaction, user) => {
  if (!reaction.message.guild || user.bot) return;
  const member = await reaction.message.guild.members.fetch(user.id).catch(() => null);
  if (member) {
    console.log(`🎙️ [NET 7: Emoji Clicker] -> Reaction array verification clicked by [${user.tag}] in [${reaction.message.guild.name}]`);
    await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_7_EMOJI_REACTION_HOOK');
  }
});

// NET 8: Sub-Thread Activity Watcher
client.on('threadMembersUpdate', async (oldMembers, newMembers) => {
  const sampleMember = newMembers.first();
  if (!sampleMember || !sampleMember.guild) return;
  
  console.log(`🎙️ [NET 8: Thread Lurker] -> Internal sub-thread synchronization data packet detected inside [${sampleMember.guild.name}]`);
  const threadMap = new Map();
  newMembers.forEach(m => {
    if (m.guildMember && !m.guildMember.user.bot) threadMap.set(m.guildMember.id, m.guildMember);
  });

  if (threadMap.size > 0) await processDiscoveredMembers(sampleMember.guild, threadMap, 'NET_8_THREAD_ROSTER_SYNC');
});

// NET 9 & NET 18: Voice Channel Connection & Stream State Monitors
client.on('voiceStateUpdate', async (oldState, newState) => {
  if (!newState.guild || !newState.member || newState.member.user.bot) return;
  
  if (newState.channelId) {
    const isMicroShift = oldState.channelId === newState.channelId;
    
    // NET 18: Microphone/Camera Twitch Watcher
    if (isMicroShift) {
      console.log(`🎙️ [NET 18: Microphone Twitch] -> Device toggle action (Mute/Stream) tracked for [${newState.member.user.tag}] in [${newState.guild.name}]`);
      await processDiscoveredMembers(newState.guild, new Map([[newState.member.id, newState.member]]), 'NET_18_VOICE_MICRO_MUTATION');
    
    // NET 9: Standard Voice Room Entry Catch
    } else {
      console.log(`🎙️ [NET 9: Voice Channel Hopper] -> Auditory grid connection slot taken by [${newState.member.user.tag}] inside [${newState.guild.name}]`);
      await processDiscoveredMembers(newState.guild, new Map([[newState.member.id, newState.member]]), 'NET_9_VOICE_ROOM_CONNECT');
    }
  }
});

// NET 10: Global User Account Profile Monitor
client.on('userUpdate', async (oldUser, newUser) => {
  if (newUser.bot) return;
  client.guilds.cache.forEach((guild) => {
    const member = guild.members.cache.get(newUser.id);
    if (member) {
      console.log(`🎙️ [NET 10: Global Identity Update] -> Global account layout adjustment synced for user [${newUser.tag}] inside [${guild.name}]`);
      processDiscoveredMembers(guild, new Map([[member.id, member]]), 'NET_10_GLOBAL_USER_PROFILE_SYNC').catch(() => {});
    }
  });
});

// NET 11, NET 12 & NET 13: Direct Raw Packet Exception Decoders
client.on('raw', async (packet) => {
  try {
    // NET 11: Direct Cache Chunk Interception Layer
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
      if (chunkMap.size > 0) {
        console.log(`🎙️ [NET 11: Raw Data Packet Decoder] -> Parsed low-level cluster chunks covering ${chunkMap.size} user blocks inside [${guild.name}]`);
        await processDiscoveredMembers(guild, chunkMap, 'NET_11_RAW_GATEWAY_CHUNK_LAYER');
      }
    }

    // NET 12: Structural Join Fallback Packet Interceptor
    if (packet.t === 'GUILD_MEMBER_ADD') {
      const { guild_id, user } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (guild && user && !user.bot) {
        console.log(`🎙️ [NET 12: Invisible Join Decoder] -> Intercepted unparsed backend entry marker for ID [${user.id}] in [${guild.name}]`);
        const m = await guild.members.fetch(user.id).catch(() => null);
        if (m) await processDiscoveredMembers(guild, new Map([[m.id, m]]), 'NET_12_RAW_STRUCTURAL_JOIN_FALLBACK');
      }
    }

    // NET 13: Structural Profile Fallback Packet Interceptor
    if (packet.t === 'GUILD_MEMBER_UPDATE') {
      const { guild_id, user } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (guild && user && !user.bot) {
        const m = await guild.members.fetch(user.id).catch(() => null);
        if (m) await processDiscoveredMembers(guild, new Map([[m.id, m]]), 'NET_13_RAW_STRUCTURAL_UPDATE_FALLBACK');
      }
    }
  } catch (err) {}
});

// NET 14: Dynamic Workspace Add Sync Hook
client.on('guildCreate', (guild) => {
  console.log(`📥 [NET 14: New Server Surveyor] -> Selfbot registered cluster attachment inside: [${guild.name}]. Initiating entry scan...`);
  setTimeout(() => pollGuildActiveEngine(guild).catch(() => {}), 5000);
});

// NET 15: Pinned Message Chat Sweep Hook
client.on('channelPinsUpdate', async (channel) => {
  if (!channel.guild) return;
  console.log(`🎙️ [NET 15: Pin-Board Sneak] -> Channel pinned layout modification tracked inside [#${channel.name}] in [${channel.guild.name}]. Scraping local chat lines...`);
  try {
    const recentMessages = await channel.messages.fetch({ limit: 10 }).catch(() => null);
    if (!recentMessages) return;

    const collectedMap = new Map();
    recentMessages.forEach(msg => {
      if (msg.member && !msg.author.bot) collectedMap.set(msg.member.id, msg.member);
    });

    if (collectedMap.size > 0) await processDiscoveredMembers(channel.guild, collectedMap, 'NET_15_CHANNEL_PIN_PROXIMITY_SWEEP');
  } catch (err) {}
});

// NET 16: Thread Initiation Trigger Hook
client.on('threadCreate', async (thread) => {
  if (!thread.guild) return;
  console.log(`🎙️ [NET 16: New Topic Watcher] -> Live sub-thread channel initialized inside server environment: [${thread.guild.name}]`);
  try {
    const ownerId = thread.ownerId;
    if (!ownerId) return;
    const member = await thread.guild.members.fetch(ownerId).catch(() => null);
    if (member && !member.user.bot) {
      await processDiscoveredMembers(thread.guild, new Map([[member.id, member]]), 'NET_16_THREAD_INITIATION_HOOK');
    }
  } catch (err) {}
});

// NET 17: Interactive Bot Grid Component Watcher
client.on('interactionCreate', async (interaction) => {
  if (!interaction.guild || !interaction.member || interaction.user.bot) return;
  console.log(`🎙️ [NET 17: Bot Button Clicker] -> Interactive app framework module triggered by [${interaction.user.tag}] inside [${interaction.guild.name}]`);
  await processDiscoveredMembers(interaction.guild, new Map([[interaction.member.id, interaction.member]]), 'NET_17_INTERACTIVE_COMPONENT_CLICK');
});

// ────────────────────────────────────────────────────────
// 🚨 MANAGEMENT EVENTS
// ────────────────────────────────────────────────────────
client.on('ready', async () => {
  console.log(`\n============================================================`);
  console.log(`🤖 FISHTANK ENGINE LOADED: ONLINE AND ACTIVE AS [${client.user.tag}]`);
  console.log(`📊 Matrix Infrastructure: 22 Specific Tracking Nets Configured Across ${client.guilds.cache.size} Servers.`);
  console.log(`============================================================`);
  START_TIME = await getStartTime();
  startActiveLoopCycle(); 
});

client.on('guildDelete', (guild) => {
  console.log(`➖ [Cluster Adjustment] Dropped connection from server [${guild.name}]. Cleaning up database footprint tables...`);
  redis.del(`guild:${guild.id}:members`).catch(() => {});
});

client.on('shardDisconnect', (event, shardId) => {
  if (event?.code === 4004) {
    console.error('============================================================');
    console.error(`🔴 CRITICAL DISCORD REJECTION (shard ${shardId}) — EXPIRED USER CONTEXT ACCOUNT TOKEN!`);
    console.error('============================================================');
  }
});

// ────────────────────────────────────────────────────────
// 🌐 LIVE HOST HEALTH CHECK & TELEMETRY API
// ────────────────────────────────────────────────────────
const app = express();
app.get('/', (req, res) => res.json({ status: 'always_fishing', running_nets: 22, tracked_guilds: client.guilds.cache.size }));
app.get('/stats', async (req, res) => {
  try {
    const guilds = [...client.guilds.cache.values()];
    const stats = [];
    for (const guild of guilds) {
      const trackedCount = await redis.scard(`guild:${guild.id}:members`);
      stats.push({ name: guild.name, id: guild.id, memberCount: guild.memberCount, savedFootprints: trackedCount || 0 });
    }
    res.json({ deployedGuildsCount: guilds.length, deploymentMatrix: stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(3000, () => console.log('✅ [HTTP Server] Local telemetry API endpoints initialized on communication port 3000'));

process.on('unhandledRejection', (err) => console.error('⚠️ [Asynchronous Crash Shield] Intercepted runtime exception:', err.message));
process.on('uncaughtException', (err) => console.error('⚠️ [Thread Loop Crash Shield] Intercepted memory process error context:', err.message));

client.login(process.env.USER_TOKEN);

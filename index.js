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

// 3. CONSTANTS (adjustable)
const GRACE_PERIOD_MS = 5 * 60 * 1000;               // 5 minutes grace for clock skew
const CHUNK_SIZE = 500;                              // members per page for bulk fetch
const BULK_POPULATION_INTERVAL_MS = 2 * 60 * 60 * 1000; // 2 hours
const TYPING_THROTTLE_MS = 60 * 1000;                // 1 minute per user per guild
const PRESENCE_THROTTLE_MS = 60 * 1000;
const DEDUP_TTL_MS = 30 * 1000;                      // 30 seconds in‑memory dedup

let START_TIME = null;
let pollingInterval = null;

// ---------- IN‑MEMORY DEDUPLICATION ----------
const recentlyProcessed = new Set();

// ---------- THROTTLE MAPS ----------
const typingThrottle = new Map();
const presenceThrottle = new Map();

// ---------- START TIME (persisted in Redis) ----------
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
    console.error('❌ [Storage Sync Error] Baseline recovery failed, using local runtime clock:', err.message);
    return Date.now();
  }
}

// ---------- WEBHOOK NOTIFICATION (with retry) ----------
async function sendNotification(payload, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const config = proxyUrl ? { httpsAgent, timeout: 10000 } : { timeout: 10000 };
      await axios.post(process.env.NOTIFY_URL, payload, config);
      console.log(`🚀 [WEBHOOK] -> SUCCESS: [${payload.username}] via [${payload.source}] in [${payload.server}]`);
      return;
    } catch (err) {
      if (i === maxRetries - 1) {
        console.error(`❌ [WEBHOOK] -> All ${maxRetries} attempts failed for [${payload.username}]:`, err.message);
      } else {
        console.log(`🔄 [WEBHOOK RETRY] -> Attempt ${i + 1}/${maxRetries} in 2000ms...`);
        await new Promise(r => setTimeout(r, 2000));
      }
    }
  }
}

// ────────────────────────────────────────────────────────
// 🎣 THE CENTRAL CORE: PROCESS AND FILTER DISCOVERED USERS
// ────────────────────────────────────────────────────────
async function processDiscoveredMembers(guild, memberMap, sourceLabel) {
  if (!START_TIME) {
    console.warn(`⚠️ [Central Processor Guard] Dropping batch from [${sourceLabel}] - Engine initializing.`);
    return;
  }
  if (!guild || !memberMap || memberMap.size === 0) return;

  const guildKey = `guild:${guild.id}:members`;
  const effectiveStart = START_TIME - GRACE_PERIOD_MS;

  const memberArray = Array.from(memberMap.values()).filter(m => m && m.user && m.id);
  if (memberArray.length === 0) return;

  // ---- DEDUPLICATION: skip users processed recently ----
  const uniqueMembers = [];
  for (const member of memberArray) {
    const key = `${guild.id}:${member.id}`;
    if (recentlyProcessed.has(key)) continue;
    recentlyProcessed.add(key);
    setTimeout(() => recentlyProcessed.delete(key), DEDUP_TTL_MS);
    uniqueMembers.push(member);
  }
  if (uniqueMembers.length === 0) return;

  // ---- Redis pipeline check ----
  const pipeline = redis.pipeline();
  for (const member of uniqueMembers) {
    pipeline.sismember(guildKey, member.id);
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

  for (let i = 0; i < uniqueMembers.length; i++) {
    const member = uniqueMembers[i];
    const isKnown = redisResults[i];

    let joinedAt = member.joinedAt?.getTime() ?? 0;
    const ageMinutes = joinedAt ? Math.floor((Date.now() - joinedAt) / 60000) : -1;

    if (!isKnown) {
      newIdsToTrack.push(member.id);

      if (joinedAt > effectiveStart) {
        console.log(`🎯 [TIMESTAMP HIT] -> NEW: [${member.user.tag}] joined ${ageMinutes} min ago via [${sourceLabel}] in [${guild.name}]`);
        notifications.push({
          server: guild.name,
          serverId: guild.id,
          userId: member.user.id,
          username: member.user.tag,
          joinedAt: new Date(joinedAt).toISOString(),
          source: sourceLabel
        });
      } else {
        if (joinedAt === 0) {
          console.log(`📦 [SILENT SAVE] -> [${member.user.tag}] has no joinedAt — saving silently`);
        } else {
          console.log(`📦 [SILENT SAVE] -> [${member.user.tag}] joined ${ageMinutes} min ago — old member, saving silently`);
        }
      }
    }
  }

  if (newIdsToTrack.length > 0) {
    try {
      await redis.sadd(guildKey, ...newIdsToTrack);
    } catch (err) {
      console.error(`❌ [CENTRAL STORAGE WRITE ERROR] Failed committing IDs for [${guild.name}]:`, err.message);
      return;
    }
  }

  if (notifications.length > 0) {
    await Promise.all(notifications.map(payload => sendNotification(payload)));
  }
}

// ────────────────────────────────────────────────────────
// 🏗️ MODE 1: BULK POPULATION ENGINE (Chunked)
// ────────────────────────────────────────────────────────
async function bulkPopulateGuild(guild) {
  console.log(`\n🏗️ [BULK BUILDER] -> Starting bulk population for [${guild.name}] (~${guild.memberCount} members)...`);
  try {
    const members = new Map();
    let lastUserId = undefined;
    let page = 0;

    while (true) {
      const options = { limit: CHUNK_SIZE, withPresences: false };
      if (lastUserId) options.after = lastUserId;

      const chunk = await guild.members.fetch(options).catch((err) => {
        console.error(`❌ [BULK BUILDER] Chunk fetch failed for [${guild.name}]:`, err.message);
        return null;
      });

      if (!chunk || chunk.size === 0) break;

      for (const [id, member] of chunk) members.set(id, member);
      lastUserId = chunk.lastKey();
      page++;
      console.log(`   Page ${page}: +${chunk.size} (total ${members.size})`);

      if (chunk.size < CHUNK_SIZE) break;
      await new Promise(r => setTimeout(r, 2000)); // delay between chunks
    }

    if (members.size === 0) {
      console.log(`⚠️ [BULK BUILDER] -> No members fetched for [${guild.name}] — skipping`);
      return;
    }

    console.log(`✅ [BULK BUILDER] -> Fetched ${members.size} members from [${guild.name}] — feeding into central processor...`);
    await processDiscoveredMembers(guild, members, 'BULK_POPULATION_ENGINE');
    console.log(`🏗️ [BULK BUILDER] -> Completed bulk population for [${guild.name}]`);
  } catch (err) {
    console.error(`❌ [BULK BUILDER CRASH] -> Failed on [${guild.name}]:`, err.message);
  }
}

async function runBulkPopulationCycle() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🏗️ [BULK POPULATION CYCLE] Starting across ${guilds.length} servers...`);
  console.log(`${'═'.repeat(60)}`);

  for (const guild of guilds) {
    await bulkPopulateGuild(guild);
    await new Promise(r => setTimeout(r, Math.floor(Math.random() * 2000) + 3000));
  }

  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🏗️ [BULK POPULATION CYCLE] Complete — Redis database populated`);
  console.log(`${'═'.repeat(60)}`);

  // Run every BULK_POPULATION_INTERVAL_MS (default 2 hours)
  setTimeout(runBulkPopulationCycle, BULK_POPULATION_INTERVAL_MS);
}

// ────────────────────────────────────────────────────────
// ⚙️ ENGINE 1: SMART ACTIVE NETS (ROUTINE SEARCH SCRAPER)
// ────────────────────────────────────────────────────────
async function pollGuildActiveEngine(guild) {
  console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  console.log(`📡 [ACTIVE LOOP ENGINE] -> Indexing: [${guild.name}] (${guild.memberCount} members)`);

  try {
    const activeGathered = new Map();
    const baseTargets = ['2026', '2025', 'sol', 'eth', 'dev', 'the', 'a', 'e', 's', 'i', 'o'];
    const growthTargets = ['crypto', 'nft', 'trade', 'alpha', 'call', 'vc', 'lfg', 'he'];

    if (guild.memberCount < 2000) {
      console.log(`🧹 [ACTIVE STRATEGY A] -> Under 2000 members. Multi-seed fishing...`);
      const randomSeeds = growthTargets.sort(() => 0.5 - Math.random()).slice(0, 3);

      for (const seed of randomSeeds) {
        console.log(`⏳ [ACTIVE ENGINE] -> Casting keyword: ['${seed}']`);
        const fetchSlice = await guild.members.fetch({ query: seed, limit: 100, time: 8000, withPresences: false }).catch((err) => {
          console.error(`❌ [Active Search Failure] Seed ['${seed}'] aborted in [${guild.name}]:`, err.message);
          return null;
        });
        if (fetchSlice && fetchSlice.size > 0) {
          console.log(`✅ [ACTIVE ENGINE] -> Found ${fetchSlice.size} via ['${seed}']`);
          fetchSlice.forEach(m => activeGathered.set(m.id, m));
        }
        await new Promise(r => setTimeout(r, 800));
      }
    } else {
      console.log(`⚠️ [ACTIVE STRATEGY B] -> Over 2000 members. Dual cross-section queries...`);

      const kw1 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      console.log(`⏳ [ACTIVE ENGINE] -> Primary pass: ['${kw1}'] (Limit: 50)`);
      const slice1 = await guild.members.fetch({ query: kw1, limit: 50, time: 10000, withPresences: false }).catch((err) => {
        console.error(`❌ [Active Search Failure] Pass 1 aborted in [${guild.name}]:`, err.message);
        return null;
      });
      if (slice1 && slice1.size > 0) {
        console.log(`✅ [ACTIVE ENGINE] -> Primary caught ${slice1.size}`);
        slice1.forEach(m => activeGathered.set(m.id, m));
      }

      await new Promise(r => setTimeout(r, 2000));

      let kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      while (kw2 === kw1) kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];

      console.log(`⏳ [ACTIVE ENGINE] -> Secondary pass: ['${kw2}'] (Limit: 50)`);
      const slice2 = await guild.members.fetch({ query: kw2, limit: 50, time: 10000, withPresences: false }).catch((err) => {
        console.error(`❌ [Active Search Failure] Pass 2 aborted in [${guild.name}]:`, err.message);
        return null;
      });
      if (slice2 && slice2.size > 0) {
        console.log(`✅ [ACTIVE ENGINE] -> Secondary caught ${slice2.size}`);
        slice2.forEach(m => activeGathered.set(m.id, m));
      }
    }

    // NET 20: Audit Log Sneak Peek
    try {
      console.log(`🕵️‍♂️ [NET 20] -> Checking audit logs for [${guild.name}]...`);
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
          console.log(`✅ [NET 20] -> Extracted ${auditMap.size} from audit logs`);
          await processDiscoveredMembers(guild, auditMap, 'NET_20_AUDIT_LOG_SNEAK');
        }
      }
    } catch (e) {
      console.error(`❌ [NET 20 EXCEPTION]:`, e.message);
    }

    const strategyLabel = guild.memberCount < 2000 ? 'ACTIVE_NET_STRATEGY_A_LOOP' : 'ACTIVE_NET_STRATEGY_B_LOOP';
    await processDiscoveredMembers(guild, activeGathered, strategyLabel);
    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

  } catch (err) {
    console.error(`❌ [ACTIVE LOOP ENGINE CRASHED] [${guild.name}]:`, err.message);
  }
}

async function startActiveLoopCycle() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🔄 [Loop Scheduler] Scanning ${guilds.length} guilds...`);
  console.log(`${'═'.repeat(60)}`);

  for (const guild of guilds) {
    try {
      await pollGuildActiveEngine(guild);
    } catch (err) {
      console.error(`❌ [Loop Scheduler Fault] [${guild.name || guild.id}]:`, err.message);
    }

    const humanDelay = Math.floor(Math.random() * (14000 - 8000 + 1) + 8000);
    console.log(`💤 [Human Delay] Pausing ${(humanDelay / 1000).toFixed(1)}s before next guild...`);
    await new Promise(r => setTimeout(r, humanDelay));
  }

  const jitteredIntervalSec = Math.floor(Math.random() * (115 - 75 + 1) + 75);
  console.log(`🏁 [Loop Scheduler] Complete. Sleeping ${jitteredIntervalSec}s...`);
  setTimeout(startActiveLoopCycle, jitteredIntervalSec * 1000);
}

// ────────────────────────────────────────────────────────
// 🔄 ENGINE 1B: ADVANCED SCHEDULE REFRESH WORKERS
// ────────────────────────────────────────────────────────

// NET 24: Channel Activity Resync
async function syncChannelMembersLoop() {
  const loopStart = Date.now();
  console.log(`⚙️ [NET 24] -> Channel activity resync starting...`);

  for (const guild of client.guilds.cache.values()) {
    try {
      let channelCount = 0;
      const validChannels = [...guild.channels.cache.values()].filter(c => c.isTextBased() && !c.isDMBased() && c.viewable);

      for (const channel of validChannels) {
        const adaptiveDelay = Math.max(500, 5000 / validChannels.length);
        await new Promise(r => setTimeout(r, adaptiveDelay));

        const recentMsgs = await channel.messages.fetch({ limit: 25 }).catch(() => null);
        if (recentMsgs && recentMsgs.size > 0) {
          const memberMap = new Map();
          recentMsgs.forEach(msg => {
            if (msg.member && !msg.author.bot) memberMap.set(msg.member.id, msg.member);
          });
          if (memberMap.size > 0) {
            await processDiscoveredMembers(guild, memberMap, 'NET_24_CHANNEL_ACTIVITY_RESYNC');
          }
        }
        channelCount++;
      }
      console.log(`✅ [NET 24] -> ${channelCount} channels swept in [${guild.name}]`);
    } catch (err) {
      console.error(`❌ [NET 24 Error] [${guild.name}]:`, err.message);
    }
  }

  const executionDuration = Date.now() - loopStart;
  const adaptiveNextInterval = Math.max(5000, 30 * 60 * 1000 - executionDuration);
  console.log(`⏱️ [NET 24] -> Finished in ${(executionDuration / 1000).toFixed(1)}s. Next in ${(adaptiveNextInterval / 1000).toFixed(1)}s`);
  setTimeout(syncChannelMembersLoop, adaptiveNextInterval);
}

// NET 27: Scheduled Events Attendee Scan
async function scanScheduledEventsLoop() {
  const loopStart = Date.now();
  console.log(`⚙️ [NET 27] -> Scheduled events scan starting...`);

  for (const guild of client.guilds.cache.values()) {
    try {
      const events = await guild.scheduledEvents.fetch().catch(() => null);
      if (!events || events.size === 0) continue;

      for (const event of events.values()) {
        const attendees = await event.fetchSubscribers().catch(() => null);
        if (attendees && attendees.size > 0) {
          const memberMap = new Map();
          for (const subscriber of attendees.values()) {
            if (!subscriber.user.bot) {
              const member = await guild.members.fetch(subscriber.user.id).catch(() => null);
              if (member) memberMap.set(member.id, member);
            }
          }
          if (memberMap.size > 0) {
            console.log(`🎙️ [NET 27] -> ${memberMap.size} attendees in [${guild.name}]`);
            await processDiscoveredMembers(guild, memberMap, 'NET_27_SCHEDULED_EVENT_SUBSCRIBERS');
          }
        }
      }
    } catch (err) {
      console.error(`❌ [NET 27 Error] [${guild.name}]:`, err.message);
    }
  }

  const executionDuration = Date.now() - loopStart;
  const adaptiveNextInterval = Math.max(5000, 20 * 60 * 1000 - executionDuration);
  console.log(`⏱️ [NET 27] -> Finished in ${(executionDuration / 1000).toFixed(1)}s. Next in ${(adaptiveNextInterval / 1000).toFixed(1)}s`);
  setTimeout(scanScheduledEventsLoop, adaptiveNextInterval);
}

// ────────────────────────────────────────────────────────
// 🎙️ ENGINE 2: PASSIVE NETS (LIVE GATEWAY PACKET LISTENERS)
// ────────────────────────────────────────────────────────

// NET 1+30: guildMemberAdd
client.on('guildMemberAdd', async (member) => {
  if (member.user.bot) return;
  console.log(`🎙️ [NET 1+30] -> [${member.user.tag}] joined [${member.guild.name}]`);
  try {
    await client.users.fetch(member.user.id, { force: true });
  } catch (err) {
    console.warn(`⚠️ [NET 30] Force profile lookup failed:`, err.message);
  }
  await processDiscoveredMembers(member.guild, new Map([[member.id, member]]), 'NET_1_GUILD_MEMBER_ADD_WITH_PROFILE_SYNC');
});

// NET 2, NET 19 & NET 26: guildMemberUpdate
client.on('guildMemberUpdate', async (oldMember, newMember) => {
  if (newMember.user.bot) return;

  const oldRoles = oldMember?.roles?.cache ?? new Map();
  const newRoles = newMember.roles.cache;

  if (newRoles.size > oldRoles.size) {
    console.log(`🎙️ [NET 19] -> Role added for [${newMember.user.tag}] in [${newMember.guild.name}]`);
    await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_19_PREMIUM_ROLE_BOOST');
    return;
  }

  const oldRoleArrayStr = [...oldRoles.keys()].sort().join(',');
  const newRoleArrayStr = [...newRoles.keys()].sort().join(',');
  if (oldRoleArrayStr !== newRoleArrayStr) {
    console.log(`🎙️ [NET 26] -> Role change for [${newMember.user.tag}] in [${newMember.guild.name}]`);
    await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_26_ROLE_CONFIG_MUTATION');
    return;
  }

  console.log(`🎙️ [NET 2] -> Profile update for [${newMember.user.tag}] in [${newMember.guild.name}]`);
  await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_2_GUILD_MEMBER_UPDATE');
});

// NET 3, 4, 22, 25: messageCreate
client.on('messageCreate', async (message) => {
  if (!message.guild) return;

  if (message.author.bot || message.webhookId) {
    const rawContent = message.content || '';
    const embedContent = message.embeds?.map(e => `${e.title || ''} ${e.description || ''}`).join(' ') || '';
    const consolidatedText = `${rawContent} ${embedContent}`;

    const idRegex = /\b(\d{17,19})\b/g;
    const matches = [...consolidatedText.matchAll(idRegex)].map(m => m[1]);

    if (matches.length > 0) {
      const parsedMap = new Map();
      for (const matchedId of matches) {
        const parsedMember = await message.guild.members.fetch(matchedId).catch(() => null);
        if (parsedMember && !parsedMember.user.bot) parsedMap.set(parsedMember.id, parsedMember);
      }
      if (parsedMap.size > 0) {
        console.log(`🎙️ [NET 22] -> ${parsedMap.size} IDs from bot message in [${message.guild.name}]`);
        await processDiscoveredMembers(message.guild, parsedMap, 'NET_22_BOT_MENTION_SCRAPER');
      }
    }

    const embedMentions = new Set();
    message.embeds.forEach(embed => {
      if (embed.footer?.text) {
        const footMatches = embed.footer.text.match(/\b(\d{17,19})\b/g);
        if (footMatches) footMatches.forEach(id => embedMentions.add(id));
      }
      if (embed.author?.name) {
        const authMatches = embed.author.name.match(/\b(\d{17,19})\b/g);
        if (authMatches) authMatches.forEach(id => embedMentions.add(id));
      }
    });

    if (embedMentions.size > 0) {
      const embedMap = new Map();
      for (const userId of embedMentions) {
        const member = await message.guild.members.fetch(userId).catch(() => null);
        if (member && !member.user.bot) embedMap.set(member.id, member);
      }
      if (embedMap.size > 0) {
        console.log(`🎙️ [NET 25] -> ${embedMap.size} from embed metadata in [${message.guild.name}]`);
        await processDiscoveredMembers(message.guild, embedMap, 'NET_25_EMBED_MENTION_EXTRACT');
      }
    }
    return;
  }

  if (message.member) {
    console.log(`🎙️ [NET 3] -> Message from [${message.author.tag}] in [${message.guild.name}]`);
    await processDiscoveredMembers(message.guild, new Map([[message.member.id, message.member]]), 'NET_3_LIVE_MESSAGE');
  }

  if (message.mentions.members.size > 0) {
    console.log(`🎙️ [NET 4] -> Mentions in [${message.guild.name}]`);
    await processDiscoveredMembers(message.guild, message.mentions.members, 'NET_4_WELCOME_SYSTEM_MENTIONS');
  }
});

// NET 5: typingStart (throttled)
client.on('typingStart', async (channel, user) => {
  if (!channel.guild || user.bot) return;
  const member = channel.guild.members.cache.get(user.id);
  if (!member) return;

  const key = `${channel.guild.id}:${user.id}`;
  const last = typingThrottle.get(key) || 0;
  if (Date.now() - last < TYPING_THROTTLE_MS) return;
  typingThrottle.set(key, Date.now());

  console.log(`🎙️ [NET 5] -> Typing from [${user.tag}] in [${channel.guild.name}]`);
  await processDiscoveredMembers(channel.guild, new Map([[member.id, member]]), 'NET_5_TYPING_INDICATOR');
});

// NET 6 & 21: presenceUpdate (throttled)
client.on('presenceUpdate', async (oldPres, newPres) => {
  if (!newPres || !newPres.guild || !newPres.member || newPres.user.bot) return;

  const key = `${newPres.guild.id}:${newPres.user.id}`;
  const last = presenceThrottle.get(key) || 0;
  if (Date.now() - last < PRESENCE_THROTTLE_MS) return;
  presenceThrottle.set(key, Date.now());

  const oldActivities = oldPres?.activities?.map(a => a.name).join(',') || '';
  const newActivities = newPres.activities?.map(a => a.name).join(',') || '';
  if (oldActivities !== newActivities) {
    console.log(`🎙️ [NET 21] -> Rich presence change for [${newPres.user.tag}] in [${newPres.guild.name}]`);
    await processDiscoveredMembers(newPres.guild, new Map([[newPres.member.id, newPres.member]]), 'NET_21_RICH_PRESENCE_SHIFT');
    return;
  }

  console.log(`🎙️ [NET 6] -> Status change for [${newPres.user.tag}] in [${newPres.guild.name}]`);
  await processDiscoveredMembers(newPres.guild, new Map([[newPres.member.id, newPres.member]]), 'NET_6_PRESENCE_STATUS_SHIFT');
});

// NET 7, 28, 29: messageReactionAdd
client.on('messageReactionAdd', async (reaction, user) => {
  if (!reaction.message.guild || user.bot) return;
  const member = await reaction.message.guild.members.fetch(user.id).catch(() => null);
  if (!member) return;

  if (reaction.emoji.id) {
    console.log(`🎙️ [NET 28] -> Custom sticker reaction from [${user.tag}] in [${reaction.message.guild.name}]`);
    await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_28_STICKER_REACTION');
  }

  if (reaction.message.embeds && reaction.message.embeds.length > 0) {
    console.log(`🎙️ [NET 29] -> Embed reaction from [${user.tag}] in [${reaction.message.guild.name}]`);
    await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_29_ROLE_SELECTOR_INTERACTION');
  }

  console.log(`🎙️ [NET 7] -> Emoji reaction from [${user.tag}] in [${reaction.message.guild.name}]`);
  await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_7_EMOJI_REACTION_HOOK');
});

// NET 8: threadMembersUpdate
client.on('threadMembersUpdate', async (oldMembers, newMembers) => {
  const sampleMember = newMembers.first();
  if (!sampleMember || !sampleMember.guild) return;

  console.log(`🎙️ [NET 8] -> Thread members update in [${sampleMember.guild.name}]`);
  const threadMap = new Map();
  newMembers.forEach(m => {
    if (m.guildMember && !m.guildMember.user.bot) threadMap.set(m.guildMember.id, m.guildMember);
  });

  if (threadMap.size > 0) await processDiscoveredMembers(sampleMember.guild, threadMap, 'NET_8_THREAD_ROSTER_SYNC');
});

// NET 9 & 18: voiceStateUpdate
client.on('voiceStateUpdate', async (oldState, newState) => {
  if (!newState.guild || !newState.member || newState.member.user.bot) return;

  if (newState.channelId) {
    const isMicroShift = oldState.channelId === newState.channelId;

    if (isMicroShift) {
      console.log(`🎙️ [NET 18] -> Voice toggle for [${newState.member.user.tag}] in [${newState.guild.name}]`);
      await processDiscoveredMembers(newState.guild, new Map([[newState.member.id, newState.member]]), 'NET_18_VOICE_MICRO_MUTATION');
    } else {
      console.log(`🎙️ [NET 9] -> Voice join from [${newState.member.user.tag}] in [${newState.guild.name}]`);
      await processDiscoveredMembers(newState.guild, new Map([[newState.member.id, newState.member]]), 'NET_9_VOICE_ROOM_CONNECT');
    }
  }
});

// NET 10: userUpdate
client.on('userUpdate', async (oldUser, newUser) => {
  if (newUser.bot) return;

  for (const guild of client.guilds.cache.values()) {
    const member = guild.members.cache.get(newUser.id);
    if (member) {
      console.log(`🎙️ [NET 10] -> Profile update for [${newUser.tag}] in [${guild.name}]`);
      await processDiscoveredMembers(guild, new Map([[member.id, member]]), 'NET_10_GLOBAL_USER_PROFILE_SYNC').catch((err) => {
        console.error(`❌ [NET 10 Error] [${guild.name}]:`, err.message);
      });
    }
  }
});

// NET 23: guildUpdate (availability recovery)
client.on('guildUpdate', async (oldGuild, newGuild) => {
  if (!oldGuild.available && newGuild.available) {
    console.log(`📡 [NET 23] -> Guild [${newGuild.name}] came back online. Syncing...`);
    try {
      const recoveredMembers = await newGuild.members.fetch().catch(() => null);
      if (recoveredMembers && recoveredMembers.size > 0) {
        await processDiscoveredMembers(newGuild, recoveredMembers, 'NET_23_GUILD_AVAILABILITY_RECOVERY');
      }
    } catch (err) {
      console.error(`❌ [NET 23] [${newGuild.name}]:`, err.message);
    }
  }
});

// NET 11, 12, 13: raw packet handlers
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
      if (chunkMap.size > 0) {
        console.log(`🎙️ [NET 11] -> Raw chunk: ${chunkMap.size} members in [${guild.name}]`);
        await processDiscoveredMembers(guild, chunkMap, 'NET_11_RAW_GATEWAY_CHUNK_LAYER');
      }
    }

    if (packet.t === 'GUILD_MEMBER_ADD') {
      const { guild_id, user } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (guild && user && !user.bot) {
        console.log(`🎙️ [NET 12] -> Raw join packet for [${user.id}] in [${guild.name}]`);
        const m = await guild.members.fetch(user.id).catch(() => null);
        if (m) await processDiscoveredMembers(guild, new Map([[m.id, m]]), 'NET_12_RAW_STRUCTURAL_JOIN_FALLBACK');
      }
    }

    if (packet.t === 'GUILD_MEMBER_UPDATE') {
      const { guild_id, user } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (guild && user && !user.bot) {
        const m = await guild.members.fetch(user.id).catch(() => null);
        if (m) await processDiscoveredMembers(guild, new Map([[m.id, m]]), 'NET_13_RAW_STRUCTURAL_UPDATE_FALLBACK');
      }
    }
  } catch (err) {
    console.error(`❌ [RAW PACKET ERROR]:`, err.message);
  }
});

// NET 14: guildCreate
client.on('guildCreate', (guild) => {
  console.log(`📥 [NET 14] -> Joined new server: [${guild.name}]. Scanning in 5s...`);
  setTimeout(() => pollGuildActiveEngine(guild).catch((err) => console.error(`❌ [NET 14 Error]:`, err.message)), 5000);
});

// NET 15: channelPinsUpdate
client.on('channelPinsUpdate', async (channel) => {
  if (!channel.guild) return;
  console.log(`🎙️ [NET 15] -> Pin update in [#${channel.name}] in [${channel.guild.name}]`);
  try {
    const recentMessages = await channel.messages.fetch({ limit: 10 }).catch(() => null);
    if (!recentMessages) return;

    const collectedMap = new Map();
    recentMessages.forEach(msg => {
      if (msg.member && !msg.author.bot) collectedMap.set(msg.member.id, msg.member);
    });

    if (collectedMap.size > 0) await processDiscoveredMembers(channel.guild, collectedMap, 'NET_15_CHANNEL_PIN_PROXIMITY_SWEEP');
  } catch (err) {
    console.error(`❌ [NET 15 Error]:`, err.message);
  }
});

// NET 16: threadCreate
client.on('threadCreate', async (thread) => {
  if (!thread.guild) return;
  console.log(`🎙️ [NET 16] -> New thread in [${thread.guild.name}]`);
  try {
    const ownerId = thread.ownerId;
    if (!ownerId) return;
    const member = await thread.guild.members.fetch(ownerId).catch(() => null);
    if (member && !member.user.bot) {
      await processDiscoveredMembers(thread.guild, new Map([[member.id, member]]), 'NET_16_THREAD_INITIATION_HOOK');
    }
  } catch (err) {
    console.error(`❌ [NET 16 Error]:`, err.message);
  }
});

// NET 17: interactionCreate
client.on('interactionCreate', async (interaction) => {
  if (!interaction.guild || !interaction.member || interaction.user.bot) return;
  console.log(`🎙️ [NET 17] -> Interaction from [${interaction.user.tag}] in [${interaction.guild.name}]`);
  await processDiscoveredMembers(interaction.guild, new Map([[interaction.member.id, interaction.member]]), 'NET_17_INTERACTIVE_COMPONENT_CLICK');
});

// ────────────────────────────────────────────────────────
// 🚨 MANAGEMENT EVENTS
// ────────────────────────────────────────────────────────
client.on('ready', async () => {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`⏳ [Startup] Testing Redis connection...`);

  try {
    await redis.get('global:start_time');
    console.log('✅ [Startup] Redis connected successfully.');
  } catch (err) {
    console.error('❌ [CRITICAL] Redis connection failed!', err.message);
    process.exit(1);
  }

  START_TIME = await getStartTime();

  console.log(`🤖 FISHTANK ENGINE: [${client.user.tag}]`);
  console.log(`📊 30 Nets + Bulk Builder across ${client.guilds.cache.size} servers`);
  console.log(`${'═'.repeat(60)}`);

  // Launch Bulk Population (5s delay)
  setTimeout(runBulkPopulationCycle, 5000);

  // Launch Active Polling Loop
  startActiveLoopCycle();

  // Launch Advanced Scheduler Background Recurrences
  setTimeout(syncChannelMembersLoop, 10000);
  setTimeout(scanScheduledEventsLoop, 30000);
});

client.on('guildDelete', (guild) => {
  console.log(`➖ [Guild Leave] [${guild.name}] — cleaning Redis...`);
  redis.del(`guild:${guild.id}:members`).catch((err) => console.error(`❌ [Guild Delete Redis Error]:`, err.message));
});

client.on('shardDisconnect', (event, shardId) => {
  if (event?.code === 4004) {
    console.error('═'.repeat(60));
    console.error(`🔴 TOKEN EXPIRED (shard ${shardId}) — Update USER_TOKEN immediately!`);
    console.error('═'.repeat(60));
  }
});

// ────────────────────────────────────────────────────────
// 🌐 HEALTH CHECK & STATS API
// ────────────────────────────────────────────────────────
const app = express();

app.get('/', (req, res) => res.json({
  status: 'always_fishing',
  running_nets: 30,
  bulk_builder: 'active',
  tracked_guilds: client.guilds.cache.size,
  timestamp: new Date().toISOString()
}));

app.get('/stats', async (req, res) => {
  try {
    const guilds = [...client.guilds.cache.values()];
    const stats = [];
    for (const guild of guilds) {
      const trackedCount = await redis.scard(`guild:${guild.id}:members`);
      stats.push({
        name: guild.name,
        id: guild.id,
        memberCount: guild.memberCount,
        savedFootprints: trackedCount || 0
      });
    }
    res.json({ deployedGuildsCount: guilds.length, deploymentMatrix: stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(3000, () => console.log('✅ [HTTP Server] Health API on port 3000'));

process.on('unhandledRejection', (err) => console.error('⚠️ [Crash Shield] Unhandled rejection:', err.message));
process.on('uncaughtException', (err) => console.error('⚠️ [Crash Shield] Uncaught exception:', err.message));

client.login(process.env.USER_TOKEN);

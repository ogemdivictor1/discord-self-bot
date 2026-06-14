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
    console.error('❌ [Storage Sync Error] Baseline recovery failed, using local runtime clock:', err.message);
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
  if (!START_TIME) {
    console.warn(`⚠️ [Central Processor Guard] Dropping batch from [${sourceLabel}] - Engine initializing.`);
    return;
  }
  if (!guild || !memberMap || memberMap.size === 0) return;

  const guildKey = `guild:${guild.id}:members`;
  const effectiveStart = START_TIME - GRACE_PERIOD_MS;
  
  const memberArray = Array.from(memberMap.values()).filter(m => m && m.user && m.id);
  if (memberArray.length === 0) return;

  const pipeline = redis.pipeline();
  for (const member of memberArray) {
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

  for (let i = 0; i < memberArray.length; i++) {
    const member = memberArray[i];
    const isKnown = redisResults[i]; 
    
    if (!member.joinedAt) continue; 
    const joinedAt = member.joinedAt.getTime();

    if (!isKnown) {
      newIdsToTrack.push(member.id);

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
    try {
      await redis.sadd(guildKey, ...newIdsToTrack);
    } catch (err) {
      console.error(`❌ [CENTRAL STORAGE WRITE ERROR] Failed committing unique IDs to Redis keyspace for [${guild.name}]:`, err.message);
      return; 
    }
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

    if (guild.memberCount < 2000) {
      console.log(`🧹 [ACTIVE STRATEGY A] -> Group size under 2000. Initiating rapid multi-seed fishing lines...`);
      const randomSeeds = growthTargets.sort(() => 0.5 - Math.random()).slice(0, 3);
      
      for (const seed of randomSeeds) {
        console.log(`⏳ [ACTIVE ENGINE] -> Casting keyword search: ['${seed}']`);
        const fetchSlice = await guild.members.fetch({ query: seed, limit: 100, time: 8000, withPresences: false }).catch((err) => {
          console.error(`❌ [Active Search Failure] Fetch for seed ['${seed}'] aborted in [${guild.name}]:`, err.message);
          return null;
        });
        if (fetchSlice && fetchSlice.size > 0) {
          console.log(`✅ [ACTIVE ENGINE SUCCESS] -> Found ${fetchSlice.size} profiles via keyword ['${seed}']`);
          fetchSlice.forEach(m => activeGathered.set(m.id, m));
        }
        await new Promise(r => setTimeout(r, 800));
      }
    } else {
      console.log(`⚠️ [ACTIVE STRATEGY B] -> Server size exceeds safety threshold. Executing dual cross-section queries...`);

      const kw1 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      console.log(`⏳ [ACTIVE ENGINE] -> Casting primary pass query: ['${kw1}'] (Limit: 50)`);
      const slice1 = await guild.members.fetch({ query: kw1, limit: 50, time: 10000, withPresences: false }).catch((err) => {
        console.error(`❌ [Active Search Failure] Pass 1 check aborted in [${guild.name}]:`, err.message);
        return null;
      });
      if (slice1 && slice1.size > 0) {
        console.log(`✅ [ACTIVE ENGINE SUCCESS] -> Primary pass ['${kw1}'] caught ${slice1.size} unique targets.`);
        slice1.forEach(m => activeGathered.set(m.id, m));
      }

      await new Promise(r => setTimeout(r, 2000));

      let kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      while (kw2 === kw1) kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];

      console.log(`⏳ [ACTIVE ENGINE] -> Casting secondary pass query: ['${kw2}'] (Limit: 50)`);
      const slice2 = await guild.members.fetch({ query: kw2, limit: 50, time: 10000, withPresences: false }).catch((err) => {
        console.error(`❌ [Active Search Failure] Pass 2 check aborted in [${guild.name}]:`, err.message);
        return null;
      });
      if (slice2 && slice2.size > 0) {
        console.log(`✅ [ACTIVE ENGINE SUCCESS] -> Secondary pass ['${kw2}'] caught ${slice2.size} unique targets.`);
        slice2.forEach(m => activeGathered.set(m.id, m));
      }
    }

    // NET 20: Audit Log Sneak Peek Check
    try {
      console.log(`🕵️‍♂️ [NET 20: Audit Log Sneak] -> Poking backend audit tables for hidden join activity footprints...`);
      const auditLogs = await guild.fetchAuditLogs({ limit: 5 }).catch((err) => {
        console.error(`❌ [NET 20 ERROR] Failed to gather structural audit records for [${guild.name}]:`, err.message);
        return null;
      });
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
    } catch (e) {
      console.error(`❌ [NET 20 EXCEPTION] Secondary process thread failed during audit parse:`, e.message);
    }

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
    try {
      await pollGuildActiveEngine(guild);
    } catch (err) {
      console.error(`❌ [Loop Scheduler Fault] Execution completely broke down on guild [${guild.name || guild.id}]:`, err.message);
    }
    
    const humanDelay = Math.floor(Math.random() * (14000 - 8000 + 1) + 8000);
    console.log(`💤 [Human Simulator Delay] Pausing for ${(humanDelay / 1000).toFixed(1)}s before checking next guild panel...`);
    await new Promise(r => setTimeout(r, humanDelay));
  }

  const jitteredIntervalSec = Math.floor(Math.random() * (115 - 75 + 1) + 75);
  console.log(`🏁 [Loop Scheduler] Run complete. Sleeping loop for ${jitteredIntervalSec}s via variable clock jitter.`);
  setTimeout(startActiveLoopCycle, jitteredIntervalSec * 1000);
}

// ────────────────────────────────────────────────────────
// 🔄 ENGINE 1B: ADVANCED SCHEDULE REFRESH WORKERS
// ────────────────────────────────────────────────────────

// NET 24: Lurker History Message Resync Engine (With Dynamic Backoff & Non-Overlapping Windows)
async function syncChannelMembersLoop() {
  const loopStart = Date.now();
  console.log(`⚙️ [NET 24: Channel Activity Resync] Initiating text backlog sweep across workspaces...`);
  
  for (const guild of client.guilds.cache.values()) {
    try {
      let channelCount = 0;
      const validChannels = [...guild.channels.cache.values()].filter(c => c.isText && !c.isDM && c.viewable);
      
      for (const channel of validChannels) {
        // Dynamic adaptive backoff configuration based on total channel visibility arrays
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
      console.log(`✅ [NET 24 Engine] Completed audit pass covering ${channelCount} total layout channels inside [${guild.name}]`);
    } catch (err) {
      console.error(`❌ [NET 24 Error] Sweep failed on guild ${guild.name}:`, err.message);
    }
  }

  const executionDuration = Date.now() - loopStart;
  const standardInterval = 30 * 60 * 1000;
  const adaptiveNextInterval = Math.max(5000, standardInterval - executionDuration);
  console.log(`⏱️ [NET 24 Loop Metrics] Run finished in ${(executionDuration / 1000).toFixed(1)}s. Rescheduling next window in ${(adaptiveNextInterval / 1000).toFixed(1)}s.`);
  setTimeout(syncChannelMembersLoop, adaptiveNextInterval);
}

// NET 27: Scheduled Discord Events Attendee Net (With Overlap Safeguards)
async function scanScheduledEventsLoop() {
  const loopStart = Date.now();
  console.log(`⚙️ [NET 27: Event Subscriber Scan] Checking server scheduled RSVP channels...`);
  
  for (const guild of client.guilds.cache.values()) {
    try {
      const events = await guild.scheduledEvents.fetch().catch(() => null);
      if (!events || events.size === 0) continue;

      for (const event of events.values()) {
        const attendees = await event.fetchSubscribers().catch((err) => {
          console.error(`❌ [NET 27 Non-Fatal] Failed parsing subscriber registry tracking for [${event.name}]:`, err.message);
          return null;
        });
        if (attendees && attendees.size > 0) {
          const memberMap = new Map();
          for (const subscriber of attendees.values()) {
            if (!subscriber.user.bot) {
              const member = await guild.members.fetch(subscriber.user.id).catch(() => null);
              if (member) memberMap.set(member.id, member);
            }
          }
          if (memberMap.size > 0) {
            console.log(`🎙️ [NET 27: Scheduled Events] -> Caught ${memberMap.size} attendees inside [${guild.name}]`);
            await processDiscoveredMembers(guild, memberMap, 'NET_27_SCHEDULED_EVENT_SUBSCRIBERS');
          }
        }
      }
    } catch (err) {
      console.error(`❌ [NET 27 Error] Event sweep failed on guild ${guild.name}:`, err.message);
    }
  }

  const executionDuration = Date.now() - loopStart;
  const standardInterval = 20 * 60 * 1000;
  const adaptiveNextInterval = Math.max(5000, standardInterval - executionDuration);
  console.log(`⏱️ [NET 27 Loop Metrics] Run finished in ${(executionDuration / 1000).toFixed(1)}s. Rescheduling next window in ${(adaptiveNextInterval / 1000).toFixed(1)}s.`);
  setTimeout(scanScheduledEventsLoop, adaptiveNextInterval);
}

// ────────────────────────────────────────────────────────
// 🎙️ ENGINE 2: PASSIVE NETS (LIVE GATEWAY PACKET LISTENERS)
// ────────────────────────────────────────────────────────

// NET 1 & NET 30 Consolidated: Single-Point Join Execution (Eliminates Race Conditions)
client.on('guildMemberAdd', async (member) => {
  if (member.user.bot) return;
  console.log(`🎙️ [NET 1 + NET 30: Join & Profile Synchronization] -> Tracking [${member.user.tag}] stepping inside [${member.guild.name}]`);
  
  try {
    // Explicit metadata layout flush from user API registry
    await client.users.fetch(member.user.id, { force: true });
  } catch (err) {
    console.warn(`⚠️ [NET 30 Warning] Force user context profile lookup failed:`, err.message);
  }

  await processDiscoveredMembers(member.guild, new Map([[member.id, member]]), 'NET_1_GUILD_MEMBER_ADD_WITH_PROFILE_SYNC');
});

// NET 2, NET 19 & NET 26: Unified Roster Role Modification and Trait Hooks
client.on('guildMemberUpdate', async (oldMember, newMember) => {
  if (newMember.user.bot) return;
  
  const oldRoles = oldMember?.roles?.cache ?? new Map();
  const newRoles = newMember.roles.cache;
  
  // NET 19: Check for pure size addition
  if (newRoles.size > oldRoles.size) {
    console.log(`🎙️ [NET 19: Hidden Channels Welcome] -> Role mutation shift isolated on [${newMember.user.tag}] in [${newMember.guild.name}]`);
    await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_19_PREMIUM_ROLE_BOOST');
    return;
  }
  
  // NET 26: Strict configuration change checking (V13 Compatible structural comparison)
  const oldRoleArrayStr = [...oldRoles.keys()].sort().join(',');
  const newRoleArrayStr = [...newRoles.keys()].sort().join(',');
  if (oldRoleArrayStr !== newRoleArrayStr) {
    console.log(`🎙️ [NET 26: Role Sync Detector] -> Complex role structure array changed for [${newMember.user.tag}] in [${newMember.guild.name}]`);
    await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_26_ROLE_CONFIG_MUTATION');
    return;
  }
  
  console.log(`🎙️ [NET 2: Profile Changer] -> Data field alteration caught for [${newMember.user.tag}] inside [${newMember.guild.name}]`);
  await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_2_GUILD_MEMBER_UPDATE');
});

// NET 3, NET 4, NET 22 & NET 25: Live Message Analytics and Multi-Vector Extraction Engine
client.on('messageCreate', async (message) => {
  if (!message.guild) return;

  // Bot & Webhook Processing Layer
  if (message.author.bot || message.webhookId) {
    const rawContent = message.content || '';
    const embedContent = message.embeds?.map(e => `${e.title || ''} ${e.description || ''}`).join(' ') || '';
    const consolidatedText = `${rawContent} ${embedContent}`;
    
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

    // NET 25: Indirect Embed Author & Footer Metadata Scraper
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
        console.log(`🎙️ [NET 25: Embed Metadata Scraper] -> Extracted ${embedMap.size} IDs from layout tags inside [${message.guild.name}]`);
        await processDiscoveredMembers(message.guild, embedMap, 'NET_25_EMBED_MENTION_EXTRACT');
      }
    }
    return;
  }

  // Human Activity Streams
  if (message.member) {
    console.log(`🎙️ [NET 3: Active Chatter] -> Chat frame dropped by [${message.author.tag}] in [#${message.channel.name}] inside [${message.guild.name}]`);
    await processDiscoveredMembers(message.guild, new Map([[message.member.id, message.member]]), 'NET_3_LIVE_MESSAGE');
  }

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
  
  const oldActivities = oldPres?.activities?.map(a => a.name).join(',') || '';
  const newActivities = newPres.activities?.map(a => a.name).join(',') || '';
  if (oldActivities !== newActivities) {
    console.log(`🎙️ [NET 21: Rich Presence Scraper] -> External client application swap tracked for [${newPres.user.tag}] in [${newPres.guild.name}]`);
    await processDiscoveredMembers(newPres.guild, new Map([[newPres.member.id, newPres.member]]), 'NET_21_RICH_PRESENCE_SHIFT');
    return;
  }

  console.log(`🎙️ [NET 6: Green Light Watcher] -> Connectivity status frame shift logged for [${newPres.user.tag}] inside [${newPres.guild.name}]`);
  await processDiscoveredMembers(newPres.guild, new Map([[newPres.member.id, newPres.member]]), 'NET_6_PRESENCE_STATUS_SHIFT');
});

// NET 7, NET 28 & NET 29: Reaction Array, Sticker Component, and Bot Embed Reaction Role Watchers
client.on('messageReactionAdd', async (reaction, user) => {
  if (!reaction.message.guild || user.bot) return;
  const member = await reaction.message.guild.members.fetch(user.id).catch(() => null);
  if (!member) return;

  // NET 28: Sticker-based reaction identifier verification
  if (reaction.emoji.id) {
    console.log(`🎙️ [NET 28: Sticker Reactor] -> Custom interactive sticker tracked from user [${user.tag}] in [${reaction.message.guild.name}]`);
    await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_28_STICKER_REACTION');
  }

  // NET 29: Verification Bot Selector Component Interception
  if (reaction.message.embeds && reaction.message.embeds.length > 0) {
    console.log(`🎙️ [NET 29: Role Assignment Trigger] -> User [${user.tag}] interacting with system menu arrays in [${reaction.message.guild.name}]`);
    await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_29_ROLE_SELECTOR_INTERACTION');
  }

  // NET 7: Standard Emoji Reaction Hook
  console.log(`🎙️ [NET 7: Emoji Clicker] -> Reaction array verification clicked by [${user.tag}] in [${reaction.message.guild.name}]`);
  await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_7_EMOJI_REACTION_HOOK');
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
    
    if (isMicroShift) {
      console.log(`🎙️ [NET 18: Microphone Twitch] -> Device toggle action (Mute/Stream) tracked for [${newState.member.user.tag}] in [${newState.guild.name}]`);
      await processDiscoveredMembers(newState.guild, new Map([[newState.member.id, newState.member]]), 'NET_18_VOICE_MICRO_MUTATION');
    } else {
      console.log(`🎙️ [NET 9: Voice Channel Hopper] -> Auditory grid connection slot taken by [${newState.member.user.tag}] inside [${newState.guild.name}]`);
      await processDiscoveredMembers(newState.guild, new Map([[newState.member.id, newState.member]]), 'NET_9_VOICE_ROOM_CONNECT');
    }
  }
});

// NET 10 Handler Fix: Implemented linear asynchronous handling across loops rather than fire-and-forget loops
client.on('userUpdate', async (oldUser, newUser) => {
  if (newUser.bot) return;
  
  for (const guild of client.guilds.cache.values()) {
    const member = guild.members.cache.get(newUser.id);
    if (member) {
      console.log(`🎙️ [NET 10: Global Identity Update] -> Global account layout adjustment synced for user [${newUser.tag}] inside [${guild.name}]`);
      await processDiscoveredMembers(guild, new Map([[member.id, member]]), 'NET_10_GLOBAL_USER_PROFILE_SYNC').catch((err) => {
        console.error(`❌ [NET 10 Error] Profiler tracking step faulted in ${guild.name}:`, err.message);
      });
    }
  }
});

// NET 23: Outage Recovery & Channel Availability Interceptor
client.on('guildUpdate', async (oldGuild, newGuild) => {
  if (!oldGuild.available && newGuild.available) {
    console.log(`📡 [NET 23: Guild Revive Sync] -> Workspace [${newGuild.name}] regained cluster connection. Syncing cached state records...`);
    try {
      const recoveredMembers = await newGuild.members.fetch().catch(() => null);
      if (recoveredMembers && recoveredMembers.size > 0) {
        await processDiscoveredMembers(newGuild, recoveredMembers, 'NET_23_GUILD_AVAILABILITY_RECOVERY');
      }
    } catch (err) {
      console.error(`❌ [NET 23 Engine Failure] State restoration aborted for [${newGuild.name}]:`, err.message);
    }
  }
});

// NET 11, NET 12 & NET 13: Direct Raw Packet Exception Decoders
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
        console.log(`🎙️ [NET 11: Raw Data Packet Decoder] -> Parsed low-level cluster chunks covering ${chunkMap.size} user blocks inside [${guild.name}]`);
        await processDiscoveredMembers(guild, chunkMap, 'NET_11_RAW_GATEWAY_CHUNK_LAYER');
      }
    }

    if (packet.t === 'GUILD_MEMBER_ADD') {
      const { guild_id, user } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (guild && user && !user.bot) {
        console.log(`🎙️ [NET 12: Invisible Join Decoder] -> Intercepted unparsed backend entry marker for ID [${user.id}] in [${guild.name}]`);
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
    console.error(`❌ [RAW PACKET PROCESSING EXCEPTION] Engine encountered fault decoding stream sequence:`, err.message);
  }
});

// NET 14: Dynamic Workspace Add Sync Hook
client.on('guildCreate', (guild) => {
  console.log(`📥 [NET 14: New Server Surveyor] -> Selfbot registered cluster attachment inside: [${guild.name}]. Initiating entry scan...`);
  setTimeout(() => pollGuildActiveEngine(guild).catch((err) => console.error(`❌ [NET 14 Error] Entry scan crash context:`, err.message)), 5000);
});

// NET 15: Pinned Message Chat Sweep Hook (With complete visibility check logs)
client.on('channelPinsUpdate', async (channel) => {
  if (!channel.guild) return;
  console.log(`🎙️ [NET 15: Pin-Board Sneak] -> Channel pinned layout modification tracked inside [#${channel.name}] in [${channel.guild.name}]. Scraping local chat lines...`);
  try {
    const recentMessages = await channel.messages.fetch({ limit: 10 }).catch((err) => {
      console.error(`❌ [NET 15 Error] Failed pinning sweep query inside channel [#${channel.name}]:`, err.message);
      return null;
    });
    if (!recentMessages) return;

    const collectedMap = new Map();
    recentMessages.forEach(msg => {
      if (msg.member && !msg.author.bot) collectedMap.set(msg.member.id, msg.member);
    });

    if (collectedMap.size > 0) await processDiscoveredMembers(channel.guild, collectedMap, 'NET_15_CHANNEL_PIN_PROXIMITY_SWEEP');
  } catch (err) {
    console.error(`❌ [NET 15 Final Catch] Unexpected runtime exception during sweep:`, err.message);
  }
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
  } catch (err) {
    console.error(`❌ [NET 16 Error] Thread parsing operation failed context:`, err.message);
  }
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
  console.log(`⏳ [Startup Verification] Testing upstream cloud dependencies...`);
  
  try {
    await redis.get('global:start_time'); 
    console.log('✅ [Dependency Check] Upstash Redis connectivity verified.');
  } catch (err) {
    console.error('❌ [CRITICAL DEPENDENCY FAULT] Upstream database validation failed!');
    console.error(`Reason: ${err.message}`);
    console.error('Terminating engine process to prevent unlogged packet leakage.');
    process.exit(1); 
  }

  START_TIME = await getStartTime(); 
  
  console.log(`🤖 FISHTANK ENGINE LOADED: ONLINE AND ACTIVE AS [${client.user.tag}]`);
  console.log(`📊 Matrix Infrastructure: 30 Specific Tracking Nets Configured Across ${client.guilds.cache.size} Servers.`);
  console.log(`============================================================`);
  
  // Launch Active Polling Loop
  startActiveLoopCycle(); 
  
  // Launch Advanced Scheduler Background Recurrences (Staggered Offsets)
  setTimeout(syncChannelMembersLoop, 10000);
  setTimeout(scanScheduledEventsLoop, 30000);
});

client.on('guildDelete', (guild) => {
  console.log(`➖ [Cluster Adjustment] Dropped connection from server [${guild.name}]. Cleaning up database footprint tables...`);
  redis.del(`guild:${guild.id}:members`).catch((err) => console.error(`❌ [Guild Eviction Database Error] Key erasure failed for [${guild.id}]:`, err.message));
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
app.get('/', (req, res) => res.json({ status: 'always_fishing', running_nets: 30, tracked_guilds: client.guilds.cache.size }));
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

const express = require('express');
const { HttpsProxyAgent } = require('https-proxy-agent');
require('dotenv').config();
const { Client } = require('discord.js-selfbot-v13');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');
const { Storage } = require('megajs');

// ─── ENV VARIABLES ───
const MEGA_EMAIL = process.env.MEGA_EMAIL || null;
const MEGA_PASSWORD = process.env.MEGA_PASSWORD || null;
const MEGA_FILE_PATH = '/data.json';

// ─── BOT CONFIG ───
const proxyUrl = process.env.PROXY_URL;
let httpsAgent = proxyUrl ? new HttpsProxyAgent(proxyUrl) : null;
const clientOptions = proxyUrl ? { ws: { agent: httpsAgent } } : {};
const client = new Client(clientOptions);

const DATA_FILE = path.join(__dirname, 'data.json');

// ─── IN‑MEMORY STATE ───
let START_TIME = null;
const guildMembers = new Map();        // guildId -> Set(userId)
const recentlyProcessed = new Set();

// ─── CONSTANTS ───
const GRACE_PERIOD_MS = 5 * 60 * 1000;
const DEDUP_TTL_MS = 30 * 1000;
const BULK_POPULATION_INTERVAL_MS = 2 * 60 * 60 * 1000; // 2 hours
const TYPING_THROTTLE_MS = 60 * 1000;
const PRESENCE_THROTTLE_MS = 60 * 1000;
const typingThrottle = new Map();
const presenceThrottle = new Map();

// ─── MEGA HELPERS ───
let megaStorage = null;

async function initMega() {
  if (!MEGA_EMAIL || !MEGA_PASSWORD) {
    console.log('⚠️ Mega credentials missing – skipping Mega backup.');
    return null;
  }
  if (!megaStorage) {
    megaStorage = new Storage({ email: MEGA_EMAIL, password: MEGA_PASSWORD });
    await new Promise((resolve, reject) => {
      megaStorage.on('ready', resolve);
      megaStorage.on('error', reject);
    });
    console.log('✅ Mega storage connected.');
  }
  return megaStorage;
}

async function downloadFromMega() {
  try {
    const storage = await initMega();
    if (!storage) return false;
    const file = storage.root.children.find(f => f.name === 'data.json');
    if (!file) {
      console.log('📭 No data.json on Mega – starting fresh.');
      return false;
    }
    const buffer = await file.downloadBuffer();
    await fs.writeFile(DATA_FILE, buffer);
    console.log('📥 Downloaded data.json from Mega.');
    return true;
  } catch (err) {
    console.error('❌ Mega download failed:', err.message);
    return false;
  }
}

async function uploadToMega() {
  try {
    const storage = await initMega();
    if (!storage) return;
    const data = await fs.readFile(DATA_FILE);
    const existing = storage.root.children.find(f => f.name === 'data.json');
    if (existing) await existing.delete();
    await storage.root.upload('data.json', data);
    console.log('📤 Uploaded data.json to Mega.');
  } catch (err) {
    console.error('❌ Mega upload failed:', err.message);
  }
}

// ─── LOCAL FILE LOAD / SAVE ───
async function loadData() {
  try {
    const raw = await fs.readFile(DATA_FILE, 'utf-8');
    const data = JSON.parse(raw);
    START_TIME = data.startTime || Date.now();
    for (const [guildId, memberArray] of Object.entries(data.guilds || {})) {
      guildMembers.set(guildId, new Set(memberArray));
    }
    console.log(`✅ Loaded data: ${guildMembers.size} guilds, start ${new Date(START_TIME).toISOString()}`);
    return true;
  } catch (err) {
    return false;
  }
}

async function saveData() {
  try {
    const data = {
      startTime: START_TIME,
      guilds: {}
    };
    for (const [guildId, membersSet] of guildMembers) {
      data.guilds[guildId] = Array.from(membersSet);
    }
    await fs.writeFile(DATA_FILE, JSON.stringify(data, null, 2));
    // Upload to Mega asynchronously (fire and forget)
    uploadToMega().catch(() => {});
    return true;
  } catch (err) {
    console.error('❌ Save failed:', err.message);
    return false;
  }
}

let saveTimeout = null;
function scheduleSave() {
  if (saveTimeout) clearTimeout(saveTimeout);
  saveTimeout = setTimeout(async () => {
    await saveData();
    saveTimeout = null;
  }, 2000);
}

// ─── STARTUP: load from Mega if local missing ───
async function initializeStorage() {
  const exists = await fs.access(DATA_FILE).then(() => true).catch(() => false);
  if (!exists) {
    console.log('📂 Local data.json missing – trying Mega...');
    const downloaded = await downloadFromMega();
    if (!downloaded) {
      START_TIME = Date.now();
      await saveData();
      console.log(`🆕 Fresh start. Start time: ${new Date(START_TIME).toISOString()}`);
      return;
    }
  }
  await loadData();
}

// ─── CENTRAL PROCESSOR ───
async function processDiscoveredMembers(guild, memberMap, sourceLabel) {
  if (!START_TIME) return;
  if (!guild || !memberMap || memberMap.size === 0) return;

  const effectiveStart = START_TIME - GRACE_PERIOD_MS;
  const guildId = guild.id;
  let membersSet = guildMembers.get(guildId);
  if (!membersSet) {
    membersSet = new Set();
    guildMembers.set(guildId, membersSet);
  }

  const memberArray = Array.from(memberMap.values()).filter(m => m && m.user && m.id);
  if (memberArray.length === 0) return;

  // Dedup
  const uniqueMembers = [];
  for (const member of memberArray) {
    const key = `${guildId}:${member.id}`;
    if (recentlyProcessed.has(key)) continue;
    recentlyProcessed.add(key);
    setTimeout(() => recentlyProcessed.delete(key), DEDUP_TTL_MS);
    uniqueMembers.push(member);
  }
  if (uniqueMembers.length === 0) return;

  const newIds = [];
  const notifications = [];

  for (const member of uniqueMembers) {
    const userId = member.id;
    if (membersSet.has(userId)) continue;

    membersSet.add(userId);
    newIds.push(userId);

    const joinedAt = member.joinedAt?.getTime() ?? 0;
    if (joinedAt > effectiveStart) {
      const ageMinutes = Math.floor((Date.now() - joinedAt) / 60000);
      console.log(`🎯 [NEW] ${member.user.tag} joined ${ageMinutes} min ago via [${sourceLabel}] in ${guild.name}`);
      notifications.push({
        server: guild.name,
        serverId: guild.id,
        userId: member.user.id,
        username: member.user.tag,
        joinedAt: new Date(joinedAt).toISOString(),
        source: sourceLabel
      });
    } else {
      console.log(`📦 [SILENT] ${member.user.tag} (old/no joinedAt) – saved silently`);
    }
  }

  if (newIds.length > 0) {
    scheduleSave();
  }

  if (notifications.length > 0) {
    await Promise.all(notifications.map(payload => sendNotification(payload)));
  }
}

// ─── WEBHOOK ───
async function sendNotification(payload, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const config = proxyUrl ? { httpsAgent, timeout: 10000 } : { timeout: 10000 };
      await axios.post(process.env.NOTIFY_URL, payload, config);
      console.log(`🚀 [WEBHOOK] SUCCESS: ${payload.username} in ${payload.server}`);
      return;
    } catch (err) {
      if (i === maxRetries - 1) console.error(`❌ Webhook failed:`, err.message);
      else await new Promise(r => setTimeout(r, 2000));
    }
  }
}

// ─── BULK POPULATION (chunked) ───
async function bulkPopulateGuild(guild) {
  console.log(`\n🏗️ [BULK] ${guild.name} (~${guild.memberCount} members)`);
  try {
    const members = new Map();
    let lastUserId = undefined;
    let page = 0;
    while (true) {
      const options = { limit: 500, withPresences: false };
      if (lastUserId) options.after = lastUserId;
      const chunk = await guild.members.fetch(options);
      if (!chunk || chunk.size === 0) break;
      for (const [id, member] of chunk) members.set(id, member);
      lastUserId = chunk.lastKey();
      page++;
      console.log(`   Page ${page}: +${chunk.size} (total ${members.size})`);
      if (chunk.size < 500) break;
      await new Promise(r => setTimeout(r, 2000));
    }
    if (members.size === 0) return;
    await processDiscoveredMembers(guild, members, 'BULK_POPULATION');
  } catch (err) {
    console.error(`❌ Bulk error ${guild.name}:`, err.message);
  }
}

async function runBulkPopulationCycle() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n🏗️ [BULK CYCLE] ${guilds.length} guilds...`);
  for (const guild of guilds) {
    await bulkPopulateGuild(guild);
    await new Promise(r => setTimeout(r, Math.random() * 2000 + 3000));
  }
  console.log(`🏗️ [BULK CYCLE] Complete. Next in ${BULK_POPULATION_INTERVAL_MS/3600000}h`);
  await uploadToMega(); // backup after cycle
  setTimeout(runBulkPopulationCycle, BULK_POPULATION_INTERVAL_MS);
}

// ─── ACTIVE LOOP (full 30‑net logic) ───
async function pollGuildActiveEngine(guild) {
  console.log(`\n📡 [ACTIVE] ${guild.name} (${guild.memberCount} members)`);
  try {
    const activeGathered = new Map();
    const baseTargets = ['2026', '2025', 'sol', 'eth', 'dev', 'the', 'a', 'e', 's', 'i', 'o'];
    const growthTargets = ['crypto', 'nft', 'trade', 'alpha', 'call', 'vc', 'lfg', 'he'];

    if (guild.memberCount < 2000) {
      const randomSeeds = growthTargets.sort(() => 0.5 - Math.random()).slice(0, 3);
      for (const seed of randomSeeds) {
        const slice = await guild.members.fetch({ query: seed, limit: 100, time: 8000, withPresences: false }).catch(() => null);
        if (slice && slice.size > 0) slice.forEach(m => activeGathered.set(m.id, m));
        await new Promise(r => setTimeout(r, 800));
      }
    } else {
      const kw1 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      const slice1 = await guild.members.fetch({ query: kw1, limit: 50, time: 10000, withPresences: false }).catch(() => null);
      if (slice1 && slice1.size > 0) slice1.forEach(m => activeGathered.set(m.id, m));
      await new Promise(r => setTimeout(r, 2000));
      let kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      while (kw2 === kw1) kw2 = baseTargets[Math.floor(Math.random() * baseTargets.length)];
      const slice2 = await guild.members.fetch({ query: kw2, limit: 50, time: 10000, withPresences: false }).catch(() => null);
      if (slice2 && slice2.size > 0) slice2.forEach(m => activeGathered.set(m.id, m));
    }

    // NET 20: Audit Log
    try {
      const auditLogs = await guild.fetchAuditLogs({ limit: 5 }).catch(() => null);
      if (auditLogs && auditLogs.entries.size > 0) {
        const auditMap = new Map();
        for (const entry of auditLogs.entries.values()) {
          if (entry.targetType === 'USER' && entry.target && !entry.target.bot) {
            const m = await guild.members.fetch(entry.target.id).catch(() => null);
            if (m) auditMap.set(m.id, m);
          }
        }
        if (auditMap.size > 0) await processDiscoveredMembers(guild, auditMap, 'NET_20_AUDIT');
      }
    } catch (e) {}

    await processDiscoveredMembers(guild, activeGathered, 'ACTIVE_LOOP');
  } catch (err) {
    console.error(`❌ Active error ${guild.name}:`, err.message);
  }
}

async function startActiveLoopCycle() {
  const guilds = [...client.guilds.cache.values()];
  console.log(`\n🔄 [ACTIVE CYCLE] ${guilds.length} guilds...`);
  for (const guild of guilds) {
    await pollGuildActiveEngine(guild);
    const delay = Math.floor(Math.random() * 6000) + 8000;
    console.log(`💤 Sleeping ${(delay/1000).toFixed(1)}s...`);
    await new Promise(r => setTimeout(r, delay));
  }
  const interval = Math.floor(Math.random() * 40) + 75; // 75–115s
  console.log(`🏁 Active cycle complete. Next in ${interval}s`);
  setTimeout(startActiveLoopCycle, interval * 1000);
}

// ─── PASSIVE NETS (ALL 30) ───

// NET 1+30: guildMemberAdd
client.on('guildMemberAdd', async (member) => {
  if (member.user.bot) return;
  console.log(`🎙️ [NET 1+30] -> ${member.user.tag} joined ${member.guild.name}`);
  try { await client.users.fetch(member.user.id, { force: true }); } catch (e) {}
  await processDiscoveredMembers(member.guild, new Map([[member.id, member]]), 'NET_1_GUILD_MEMBER_ADD');
});

// NET 2, 19, 26: guildMemberUpdate
client.on('guildMemberUpdate', async (oldMember, newMember) => {
  if (newMember.user.bot) return;
  const oldRoles = oldMember?.roles?.cache ?? new Map();
  const newRoles = newMember.roles.cache;
  if (newRoles.size > oldRoles.size) {
    await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_19_ROLE_BOOST');
    return;
  }
  const oldStr = [...oldRoles.keys()].sort().join(',');
  const newStr = [...newRoles.keys()].sort().join(',');
  if (oldStr !== newStr) {
    await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_26_ROLE_CHANGE');
    return;
  }
  await processDiscoveredMembers(newMember.guild, new Map([[newMember.id, newMember]]), 'NET_2_PROFILE_UPDATE');
});

// NET 3, 4, 22, 25: messageCreate
client.on('messageCreate', async (message) => {
  if (!message.guild) return;
  if (message.author.bot || message.webhookId) {
    const raw = message.content || '';
    const embedText = message.embeds?.map(e => `${e.title||''} ${e.description||''}`).join(' ') || '';
    const text = `${raw} ${embedText}`;
    const ids = [...text.matchAll(/\b(\d{17,19})\b/g)].map(m => m[1]);
    const map = new Map();
    for (const id of ids) {
      const m = await message.guild.members.fetch(id).catch(() => null);
      if (m && !m.user.bot) map.set(m.id, m);
    }
    if (map.size > 0) await processDiscoveredMembers(message.guild, map, 'NET_22_BOT_MENTION');

    const embedMentions = new Set();
    message.embeds.forEach(embed => {
      if (embed.footer?.text) {
        const f = embed.footer.text.match(/\b(\d{17,19})\b/g);
        if (f) f.forEach(id => embedMentions.add(id));
      }
      if (embed.author?.name) {
        const a = embed.author.name.match(/\b(\d{17,19})\b/g);
        if (a) a.forEach(id => embedMentions.add(id));
      }
    });
    const embedMap = new Map();
    for (const id of embedMentions) {
      const m = await message.guild.members.fetch(id).catch(() => null);
      if (m && !m.user.bot) embedMap.set(m.id, m);
    }
    if (embedMap.size > 0) await processDiscoveredMembers(message.guild, embedMap, 'NET_25_EMBED_EXTRACT');
    return;
  }
  if (message.member) {
    await processDiscoveredMembers(message.guild, new Map([[message.member.id, message.member]]), 'NET_3_MESSAGE');
  }
  if (message.mentions.members.size > 0) {
    await processDiscoveredMembers(message.guild, message.mentions.members, 'NET_4_MENTIONS');
  }
});

// NET 5: typingStart (throttled)
client.on('typingStart', async (channel, user) => {
  if (!channel.guild || user.bot) return;
  const member = channel.guild.members.cache.get(user.id);
  if (!member) return;
  const key = `${channel.guild.id}:${user.id}`;
  if (Date.now() - (typingThrottle.get(key) || 0) < TYPING_THROTTLE_MS) return;
  typingThrottle.set(key, Date.now());
  await processDiscoveredMembers(channel.guild, new Map([[member.id, member]]), 'NET_5_TYPING');
});

// NET 6 & 21: presenceUpdate (throttled)
client.on('presenceUpdate', async (oldPres, newPres) => {
  if (!newPres || !newPres.guild || !newPres.member || newPres.user.bot) return;
  const key = `${newPres.guild.id}:${newPres.user.id}`;
  if (Date.now() - (presenceThrottle.get(key) || 0) < PRESENCE_THROTTLE_MS) return;
  presenceThrottle.set(key, Date.now());
  const oldAct = oldPres?.activities?.map(a => a.name).join(',') || '';
  const newAct = newPres.activities?.map(a => a.name).join(',') || '';
  if (oldAct !== newAct) {
    await processDiscoveredMembers(newPres.guild, new Map([[newPres.member.id, newPres.member]]), 'NET_21_RICH_PRESENCE');
    return;
  }
  await processDiscoveredMembers(newPres.guild, new Map([[newPres.member.id, newPres.member]]), 'NET_6_PRESENCE');
});

// NET 7, 28, 29: messageReactionAdd
client.on('messageReactionAdd', async (reaction, user) => {
  if (!reaction.message.guild || user.bot) return;
  const member = await reaction.message.guild.members.fetch(user.id).catch(() => null);
  if (!member) return;
  if (reaction.emoji.id) {
    await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_28_STICKER');
  }
  if (reaction.message.embeds && reaction.message.embeds.length > 0) {
    await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_29_EMBED_REACTION');
  }
  await processDiscoveredMembers(reaction.message.guild, new Map([[member.id, member]]), 'NET_7_REACTION');
});

// NET 8: threadMembersUpdate
client.on('threadMembersUpdate', async (oldMembers, newMembers) => {
  const sample = newMembers.first();
  if (!sample || !sample.guild) return;
  const map = new Map();
  newMembers.forEach(m => {
    if (m.guildMember && !m.guildMember.user.bot) map.set(m.guildMember.id, m.guildMember);
  });
  if (map.size > 0) await processDiscoveredMembers(sample.guild, map, 'NET_8_THREAD');
});

// NET 9 & 18: voiceStateUpdate
client.on('voiceStateUpdate', async (oldState, newState) => {
  if (!newState.guild || !newState.member || newState.member.user.bot) return;
  if (newState.channelId) {
    const sameChannel = oldState.channelId === newState.channelId;
    if (sameChannel) {
      await processDiscoveredMembers(newState.guild, new Map([[newState.member.id, newState.member]]), 'NET_18_VOICE_TOGGLE');
    } else {
      await processDiscoveredMembers(newState.guild, new Map([[newState.member.id, newState.member]]), 'NET_9_VOICE_JOIN');
    }
  }
});

// NET 10: userUpdate
client.on('userUpdate', async (oldUser, newUser) => {
  if (newUser.bot) return;
  for (const guild of client.guilds.cache.values()) {
    const member = guild.members.cache.get(newUser.id);
    if (member) {
      await processDiscoveredMembers(guild, new Map([[member.id, member]]), 'NET_10_PROFILE_SYNC');
    }
  }
});

// NET 23: guildUpdate
client.on('guildUpdate', async (oldGuild, newGuild) => {
  if (!oldGuild.available && newGuild.available) {
    console.log(`📡 [NET 23] Guild ${newGuild.name} came online. Syncing...`);
    const recovered = await newGuild.members.fetch().catch(() => null);
    if (recovered && recovered.size > 0) {
      await processDiscoveredMembers(newGuild, recovered, 'NET_23_RECOVERY');
    }
  }
});

// NET 11, 12, 13: raw packets
client.on('raw', async (packet) => {
  try {
    if (packet.t === 'GUILD_MEMBERS_CHUNK') {
      const { guild_id, members } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (!guild || !members) return;
      const map = new Map();
      for (const data of members) {
        if (data.user && !data.user.bot) {
          const m = await guild.members.fetch(data.user.id).catch(() => null);
          if (m) map.set(m.id, m);
        }
      }
      if (map.size > 0) await processDiscoveredMembers(guild, map, 'NET_11_CHUNK');
    }
    if (packet.t === 'GUILD_MEMBER_ADD') {
      const { guild_id, user } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (guild && user && !user.bot) {
        const m = await guild.members.fetch(user.id).catch(() => null);
        if (m) await processDiscoveredMembers(guild, new Map([[m.id, m]]), 'NET_12_RAW_JOIN');
      }
    }
    if (packet.t === 'GUILD_MEMBER_UPDATE') {
      const { guild_id, user } = packet.d;
      const guild = client.guilds.cache.get(guild_id);
      if (guild && user && !user.bot) {
        const m = await guild.members.fetch(user.id).catch(() => null);
        if (m) await processDiscoveredMembers(guild, new Map([[m.id, m]]), 'NET_13_RAW_UPDATE');
      }
    }
  } catch (e) {}
});

// NET 14: guildCreate
client.on('guildCreate', (guild) => {
  console.log(`📥 [NET 14] Joined ${guild.name}`);
  setTimeout(() => pollGuildActiveEngine(guild), 5000);
});

// NET 15: channelPinsUpdate
client.on('channelPinsUpdate', async (channel) => {
  if (!channel.guild) return;
  const msgs = await channel.messages.fetch({ limit: 10 }).catch(() => null);
  if (!msgs) return;
  const map = new Map();
  msgs.forEach(msg => { if (msg.member && !msg.author.bot) map.set(msg.member.id, msg.member); });
  if (map.size > 0) await processDiscoveredMembers(channel.guild, map, 'NET_15_PINS');
});

// NET 16: threadCreate
client.on('threadCreate', async (thread) => {
  if (!thread.guild) return;
  const ownerId = thread.ownerId;
  if (!ownerId) return;
  const member = await thread.guild.members.fetch(ownerId).catch(() => null);
  if (member && !member.user.bot) {
    await processDiscoveredMembers(thread.guild, new Map([[member.id, member]]), 'NET_16_THREAD_CREATE');
  }
});

// NET 17: interactionCreate
client.on('interactionCreate', async (interaction) => {
  if (!interaction.guild || !interaction.member || interaction.user.bot) return;
  await processDiscoveredMembers(interaction.guild, new Map([[interaction.member.id, interaction.member]]), 'NET_17_INTERACTION');
});

// ─── READY EVENT ───
client.on('ready', async () => {
  console.log(`\n🤖 Selfbot: ${client.user.tag} | ${client.guilds.cache.size} servers`);
  await initializeStorage();

  // Launch bulk population after 5s
  setTimeout(runBulkPopulationCycle, 5000);
  // Launch active loop after 10s
  setTimeout(startActiveLoopCycle, 10000);
});

// ─── GRACEFUL SHUTDOWN ───
async function shutdown() {
  console.log('\n💾 Saving data before exit...');
  await saveData();
  await uploadToMega();
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ─── HEALTH SERVER ───
const app = express();
app.get('/', (req, res) => res.json({ status: 'running', guilds: client.guilds.cache.size }));
app.get('/stats', (req, res) => {
  const stats = {};
  for (const [guildId, membersSet] of guildMembers) {
    const guild = client.guilds.cache.get(guildId);
    stats[guild?.name || guildId] = { trackedMembers: membersSet.size };
  }
  res.json({ totalGuilds: guildMembers.size, startTime: new Date(START_TIME).toISOString() });
});
app.listen(3000, () => console.log('✅ Health server on :3000'));

process.on('unhandledRejection', (err) => console.error('⚠️ Unhandled rejection:', err.message));
process.on('uncaughtException', (err) => console.error('⚠️ Uncaught exception:', err.message));

client.login(process.env.USER_TOKEN);

require('dotenv').config();
const { Client } = require('discord.js-selfbot-v13');
const axios = require('axios');

const client = new Client();

const memberCounts = new Map();
const memberSnapshots = new Map();
let pollingTimeout = null;

// Random interval between 20-40 seconds
function getRandomInterval() {
  return Math.floor(Math.random() * (40000 - 20000 + 1)) + 20000;
}

// Random delay utility
function randomDelay(min, max) {
  const ms = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Simulate user activity occasionally
async function simulateActivity() {
  if (Math.random() > 0.7) {
    client.user.setStatus('online');
    console.log('📍 Simulated activity');
  }
}

// Send notification with retry logic
async function sendNotificationWithRetry(payload, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      await axios.post(process.env.NOTIFY_URL, payload);
      console.log('✅ Notification sent!');
      return;
    } catch (err) {
      if (i < maxRetries - 1) {
        await randomDelay(1000, 3000);
        console.log(`🔄 Retrying notification... (${i + 1}/${maxRetries})`);
      } else {
        console.error(`❌ Failed to notify after ${maxRetries} attempts:`, err.message);
      }
    }
  }
}

async function initializeGuild(guild) {
  try {
    const members = await guild.members.fetch();
    memberCounts.set(guild.id, members.size);
    const memberIds = new Set(members.map(m => m.user.id));
    memberSnapshots.set(guild.id, memberIds);
    console.log(`✅ Monitoring ${guild.name}: ${members.size} members`);
  } catch (err) {
    console.error(`❌ Failed to initialize ${guild.name}:`, err.message);
  }
}

async function pollGuild(guild) {
  try {
    const members = await guild.members.fetch();
    const previousCount = memberCounts.get(guild.id) || 0;
    const currentCount = members.size;
    const previousSnapshot = memberSnapshots.get(guild.id) || new Set();

    if (currentCount > previousCount) {
      const newMembers = members.filter(m => !previousSnapshot.has(m.user.id));

      for (const member of newMembers.values()) {
        const payload = {
          server: guild.name,
          serverId: guild.id,
          username: member.user.tag,
          userId: member.user.id,
          joinedAt: member.joinedAt ? member.joinedAt.toISOString() : new Date().toISOString(),
        };

        console.log(`🆕 New member in ${guild.name}: ${member.user.tag}`);

        // Small jitter before notifying
        await randomDelay(500, 1500);
        await sendNotificationWithRetry(payload);
      }

      const newSnapshot = new Set(members.map(m => m.user.id));
      memberSnapshots.set(guild.id, newSnapshot);
      memberCounts.set(guild.id, currentCount);

    } else if (currentCount < previousCount) {
      const newSnapshot = new Set(members.map(m => m.user.id));
      memberSnapshots.set(guild.id, newSnapshot);
      memberCounts.set(guild.id, currentCount);
      console.log(`👋 Member left ${guild.name}. New count: ${currentCount}`);

    } else {
      console.log(`[${guild.name}] No changes. Members: ${currentCount}`);
    }

  } catch (err) {
    console.error(`⚠️ Polling error for ${guild.name}:`, err.message);
  }
}

// Recursive polling with new random interval each cycle
async function startPolling() {
  const interval = getRandomInterval();
  console.log(`⏱️ Next poll in ${(interval / 1000).toFixed(1)}s`);

  pollingTimeout = setTimeout(async () => {

    // 15% chance to skip this cycle
    if (Math.random() > 0.85) {
      console.log('⏭️ Skipping this poll cycle');
      startPolling();
      return;
    }

    console.log(`[${new Date().toLocaleTimeString()}] Polling... checking all servers`);

    // Simulate activity occasionally
    await simulateActivity();

    const guilds = [...client.guilds.cache.values()];

    for (const guild of guilds) {
      await pollGuild(guild);
      await randomDelay(200, 500); // stagger between guilds
    }

    startPolling(); // new random interval each cycle
  }, interval);
}

client.on('ready', async () => {
  console.log(`Selfbot running as ${client.user.tag}`);
  console.log(`Monitoring ${client.guilds.cache.size} servers`);

  const guilds = [...client.guilds.cache.values()];

  for (const guild of guilds) {
    await initializeGuild(guild);
    await randomDelay(500, 1000);
  }

  console.log('✅ All servers initialized. Polling started...');
  startPolling();
});

// Auto add new server when you join
client.on('guildCreate', async (guild) => {
  console.log(`📡 Joined new server: ${guild.name}`);
  await initializeGuild(guild);
  console.log(`✅ ${guild.name} added to monitoring automatically!`);
});

// Auto remove server when you leave
client.on('guildDelete', (guild) => {
  memberCounts.delete(guild.id);
  memberSnapshots.delete(guild.id);
  console.log(`🚪 Left: ${guild.name}. Removed from monitoring.`);
});

// Cleanup on disconnect
client.on('disconnect', () => {
  console.log('⚠️ Bot disconnected. Stopping polling...');
  if (pollingTimeout) clearTimeout(pollingTimeout);
});

// Catch all errors without crashing
process.on('unhandledRejection', (err) => {
  console.error('⚠️ Unhandled error:', err.message);
});

process.on('uncaughtException', (err) => {
  console.error('⚠️ Uncaught exception:', err.message);
});

client.login(process.env.USER_TOKEN);
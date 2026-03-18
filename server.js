const express = require("express");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(express.json({ limit: "1mb" }));

// ─── GitHub persistence config ───
const GH_TOKEN = process.env.GH_TOKEN;
const GH_REPO = process.env.GH_REPO;
const GH_BRANCH = process.env.GH_BRANCH || "main";

// ─── Slack config ───
const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const SLACK_CHANNEL_ID = process.env.SLACK_CHANNEL_ID; // e.g. "C01234ABCDE"
const SLACK_SIGNING_SECRET = process.env.SLACK_SIGNING_SECRET;

// ─── Favro API config ───
const FAVRO_EMAIL = process.env.FAVRO_EMAIL;
const FAVRO_API_TOKEN = process.env.FAVRO_API_TOKEN;
const FAVRO_ORG_ID = process.env.FAVRO_ORG_ID;
const FAVRO_WIDGET_ID = process.env.FAVRO_WIDGET_ID; // optional: limit to specific board

// ─── GA4 config ───
const GA4_PROPERTY_ID = process.env.GA4_PROPERTY_ID || "196785998";
const GA4_SERVICE_ACCOUNT_JSON = process.env.GA4_SERVICE_ACCOUNT_JSON; // full JSON string

// ─── Experiment tag to watch ───
const EXPERIMENT_TAG = (process.env.EXPERIMENT_TAG || "experiment").toLowerCase();

// ─── Column config ───
const COLUMNS = {
  "f98217af6ec2a0bf271f8974": { name: "Epic", order: 0 },
  "af8ed7601472c3e22faa634b": { name: "Backlog", order: 1 },
  "450eca361acacbda92cde88e": { name: "Needs Action", order: 2 },
  "15563c2c50d3838a64e76261": { name: "Ready For Dev", order: 3 },
  "ddfdfc6d1de72aea01fb80e4": { name: "Doing", order: 4 },
  "3f6c5115c9ed29fb4c8e6490": { name: "Code Review", order: 5 },
  "4405671fd8390707bc5b4bd2": { name: "Waits for Build", order: 6 },
  "9ee2b845e4e8262a6ef80c56": { name: "QA Todo", order: 7 },
  "e70873a4ee5289566911d143": { name: "QA Doing", order: 8 },
  "fa1aeb43eb0e9637b68514b0": { name: "Ready for Release", order: 9 },
  "6703c8fa583bbc9c1f98caa8": { name: "Released", order: 10 },
  "bcd5f846b2d9a470240c4f18": { name: "Analytics", order: 11 },
  "e2179d7ed64f5b116d58803e": { name: "SEO/PM analysis", order: 12 },
  "9f67ac81338248202d3fa2da": { name: "Done", order: 13 },
};

const USERS = {
  G6HTMaWShYggd8sqD: "Almira Ibrayeva",
  GyKDJiGdbYawukJCo: "Alex Astapenko",
  K3jeJTGMJ8xAxLWHB: "Mike Samokhvalov",
  MYPjijek85GEWMtbC: "Alina Strelnikova",
  hvJny3G5scPXG9pq4: "Anna Shcharbakova",
  MC5akMsHGKGRByeTp: "Margarita Artemieva",
  QRFQ7T3xrTRDhNdyo: "Egor Yudin",
  bQyqbKcgarcZdLTLF: "Ismat Valiyev",
  "2xX9CfJy2WqmfBP6q": "Elena Sharipova",
  uJgkr4vdbd4nDEJrr: "Anton Pavlov",
  dx4gb956trr5snDcK: "Mashxurbek Muhammadjonov",
  REqPJT3DtdgqzKQci: "Dmitrii Pavlov",
  agWL5iCQmgHA5bnFK: "Alexandr Karyakin",
  ptjjRXNz9Hk8Zxetj: "Radik Biktagirov",
  "5FPNfZyszJ7uw2AAm": "Aldar Dorzhiev",
  nTovtv7LRYycB9ABH: "Alexander Pyatkin",
  oPxPaCZceL3y9ueN2: "AbdulMalik Akhzamov",
  DYtSRWkgu4AqxXqHi: "Valeriy Puzakov",
  QrosmcmqpJLX8nnRR: "Daniil Kuznetsov",
  HMm5khuxTieWp5nSF: "Konstantin Zaytsev",
  AWJ67tbK9ozbKrxSC: "Sergey Ivontev",
  jo599ywJ26dNPJyPr: "Victoria Lomonosova",
  bBLTEoRavz3bu2CEy: "Zafar Yertayev",
  A9zXoGNSfKEDt2rA7: "Selim Ataballyev",
};

const SIGNIFICANT_RETURNS = new Set([
  "Code Review -> Doing",
  "Code Review -> Ready For Dev",
  "Waits for Build -> Doing",
  "Waits for Build -> Ready For Dev",
  "QA Todo -> Doing",
  "QA Todo -> Ready For Dev",
  "QA Doing -> Doing",
  "QA Doing -> Ready For Dev",
  "QA Doing -> QA Todo",
  "Ready for Release -> Doing",
  "Ready for Release -> Ready For Dev",
  "Ready for Release -> QA Todo",
  "Ready for Release -> QA Doing",
]);

// ─── In-memory state ───
const cardState = {};
const allEvents = [];
const returns = [];
const experiments = {}; // cardCommonId → experiment state

// ─── GitHub API helpers ───
async function ghApiRequest(method, filePath, body = null) {
  if (!GH_TOKEN || !GH_REPO) return null;
  const url = `https://api.github.com/repos/${GH_REPO}/contents/data/${filePath}`;
  const headers = {
    Authorization: `Bearer ${GH_TOKEN}`,
    Accept: "application/vnd.github.v3+json",
    "User-Agent": "favro-webhook-logger",
  };
  if (body) headers["Content-Type"] = "application/json";
  const opts = { method, headers };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(url, opts);
  if (!res.ok && res.status !== 404) {
    console.error(`GitHub API error ${res.status} for ${filePath}:`, await res.text());
    return null;
  }
  if (res.status === 404) return null;
  return res.json();
}

async function ghReadFile(filePath) {
  const data = await ghApiRequest("GET", filePath);
  if (!data || !data.content) return null;
  const content = Buffer.from(data.content, "base64").toString("utf8");
  return { content: JSON.parse(content), sha: data.sha };
}

async function ghWriteFile(filePath, jsonData, sha) {
  const content = Buffer.from(JSON.stringify(jsonData, null, 2)).toString("base64");
  const body = {
    message: `auto: update ${filePath} [${new Date().toISOString()}]`,
    content,
    branch: GH_BRANCH,
  };
  if (sha) body.sha = sha;
  return ghApiRequest("PUT", filePath, body);
}

const fileShas = {};

async function loadStateFromGitHub() {
  if (!GH_TOKEN || !GH_REPO) {
    console.log("⚠ GH_TOKEN or GH_REPO not set — GitHub persistence disabled");
    return;
  }
  console.log(`Loading state from GitHub (${GH_REPO})...`);
  try {
    const stateResult = await ghReadFile("card_state.json");
    if (stateResult) {
      Object.assign(cardState, stateResult.content);
      fileShas["card_state.json"] = stateResult.sha;
      console.log(`  Loaded state for ${Object.keys(stateResult.content).length} cards`);
    }
    const eventsResult = await ghReadFile("events.json");
    if (eventsResult) {
      allEvents.push(...eventsResult.content);
      fileShas["events.json"] = eventsResult.sha;
      console.log(`  Loaded ${eventsResult.content.length} events`);
    }
    const returnsResult = await ghReadFile("returns.json");
    if (returnsResult) {
      returns.push(...returnsResult.content);
      fileShas["returns.json"] = returnsResult.sha;
      console.log(`  Loaded ${returnsResult.content.length} returns`);
    }
    const experimentsResult = await ghReadFile("experiments.json");
    if (experimentsResult) {
      Object.assign(experiments, experimentsResult.content);
      fileShas["experiments.json"] = experimentsResult.sha;
      console.log(`  Loaded ${Object.keys(experimentsResult.content).length} experiments`);
    }
    console.log("GitHub state loaded successfully");
  } catch (e) {
    console.error("Error loading from GitHub:", e.message);
  }
}

let saveTimeout = null;
const SAVE_DELAY_MS = 5000;

function scheduleSaveToGitHub() {
  if (!GH_TOKEN || !GH_REPO) return;
  if (saveTimeout) clearTimeout(saveTimeout);
  saveTimeout = setTimeout(() => saveToGitHub(), SAVE_DELAY_MS);
}

async function saveToGitHub() {
  if (!GH_TOKEN || !GH_REPO) return;
  console.log("Saving state to GitHub...");
  try {
    const files = [
      { name: "card_state.json", data: cardState },
      { name: "events.json", data: allEvents },
      { name: "returns.json", data: returns },
      { name: "experiments.json", data: experiments },
    ];
    for (const f of files) {
      const result = await ghWriteFile(f.name, f.data, fileShas[f.name]);
      if (result && result.content) {
        fileShas[f.name] = result.content.sha;
      }
    }
    console.log("  GitHub save complete");
  } catch (e) {
    console.error("Error saving to GitHub:", e.message);
  }
}

const DATA_DIR = path.join(__dirname, "data");
function saveStateLocal() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(path.join(DATA_DIR, "card_state.json"), JSON.stringify(cardState, null, 2));
    fs.writeFileSync(path.join(DATA_DIR, "events.json"), JSON.stringify(allEvents, null, 2));
    fs.writeFileSync(path.join(DATA_DIR, "returns.json"), JSON.stringify(returns, null, 2));
    fs.writeFileSync(path.join(DATA_DIR, "experiments.json"), JSON.stringify(experiments, null, 2));
  } catch (e) {
    console.error("Local save failed:", e.message);
  }
}

// ─── Helpers ───
function colName(colId) { return COLUMNS[colId]?.name || colId; }
function colOrder(colId) { return COLUMNS[colId]?.order ?? -1; }
function userName(userId) { return USERS[userId] || userId; }
function classifyReturn(fromCol) {
  if (fromCol.includes("Code Review")) return "code_review_rejection";
  if (fromCol.includes("QA")) return "qa_rejection";
  if (fromCol.includes("Waits for Build")) return "build_rejection";
  if (fromCol.includes("Ready for Release")) return "release_rejection";
  return "other_return";
}

// ─── Slack helpers ───
async function postToSlack(text, threadTs = null) {
  if (!SLACK_BOT_TOKEN || !SLACK_CHANNEL_ID) {
    console.log("[SLACK MOCK]", text);
    return { ts: Date.now().toString(), ok: true };
  }
  const body = { channel: SLACK_CHANNEL_ID, text };
  if (threadTs) body.thread_ts = threadTs;
  try {
    const res = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${SLACK_BOT_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!data.ok) console.error("Slack error:", data.error);
    return data;
  } catch (e) {
    console.error("Slack request failed:", e.message);
    return null;
  }
}

// ─── Favro API helpers ───
function favroHeaders() {
  const creds = Buffer.from(`${FAVRO_EMAIL}:${FAVRO_API_TOKEN}`).toString("base64");
  return {
    Authorization: `Basic ${creds}`,
    "organizationId": FAVRO_ORG_ID,
    "Content-Type": "application/json",
  };
}

async function addFavroComment(cardCommonId, commentText) {
  if (!FAVRO_EMAIL || !FAVRO_API_TOKEN || !FAVRO_ORG_ID) {
    console.log("[FAVRO MOCK] Comment on", cardCommonId, ":", commentText.slice(0, 80));
    return { ok: true };
  }
  try {
    const res = await fetch("https://favro.com/api/1.0/comments", {
      method: "POST",
      headers: favroHeaders(),
      body: JSON.stringify({ cardCommonId, comment: commentText }),
    });
    const data = await res.json();
    if (data.errors) console.error("Favro comment error:", data.errors);
    return data;
  } catch (e) {
    console.error("Favro request failed:", e.message);
    return null;
  }
}

// ─── GA4 Data API helper ───
// Calls GA4 Data API using a service account (JWT auth)
async function getGA4EventCounts(eventNames, daysAgo = 7) {
  if (!GA4_SERVICE_ACCOUNT_JSON) {
    console.log("[GA4 MOCK] Would check events:", eventNames);
    return null;
  }
  try {
    const sa = JSON.parse(GA4_SERVICE_ACCOUNT_JSON);
    const token = await getServiceAccountToken(sa, "https://www.googleapis.com/auth/analytics.readonly");

    const res = await fetch(
      `https://analyticsdata.googleapis.com/v1beta/properties/${GA4_PROPERTY_ID}:runReport`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          dateRanges: [{ startDate: `${daysAgo}daysAgo`, endDate: "yesterday" }],
          dimensions: [{ name: "eventName" }],
          metrics: [{ name: "eventCount" }, { name: "totalUsers" }],
          dimensionFilter: {
            filter: {
              fieldName: "eventName",
              inListFilter: { values: eventNames },
            },
          },
        }),
      }
    );
    const data = await res.json();
    if (data.error) {
      console.error("GA4 API error:", data.error.message);
      return null;
    }
    // Parse rows
    const results = {};
    for (const row of data.rows || []) {
      const name = row.dimensionValues[0].value;
      const count = parseInt(row.metricValues[0].value) || 0;
      const users = parseInt(row.metricValues[1].value) || 0;
      results[name] = { count, users };
    }
    return results;
  } catch (e) {
    console.error("GA4 request failed:", e.message);
    return null;
  }
}

// Minimal JWT implementation for service account auth
async function getServiceAccountToken(sa, scope) {
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const claim = {
    iss: sa.client_email,
    scope,
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now,
  };
  const encode = (obj) => Buffer.from(JSON.stringify(obj)).toString("base64url");
  const unsigned = `${encode(header)}.${encode(claim)}`;

  // Sign with RS256
  const { createSign } = require("crypto");
  const sign = createSign("RSA-SHA256");
  sign.update(unsigned);
  const signature = sign.sign(sa.private_key, "base64url");
  const jwt = `${unsigned}.${signature}`;

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });
  const data = await res.json();
  if (!data.access_token) throw new Error(`Token error: ${JSON.stringify(data)}`);
  return data.access_token;
}

// ─── Format events table for Favro comment ───
function formatEventsTable(events) {
  const header = "**Analytics Events**\n\n| Event | Description | Status |\n|---|---|---|";
  const rows = events.map((e) => `| \`${e.name}\` | ${e.description || "—"} | ⬜ Planned |`);
  return [header, ...rows].join("\n");
}

// ─── Parse events from Slack message text ───
// Expects lines like: "- event_name: description" or "event_name - description"
function parseEventsFromText(text) {
  const lines = text.split("\n");
  const events = [];
  for (const line of lines) {
    const trimmed = line.trim().replace(/^[-*•]\s*/, "");
    if (!trimmed) continue;

    // Format: event_name: description
    const colonMatch = trimmed.match(/^([a-z_][a-z0-9_]*)\s*:\s*(.+)$/i);
    if (colonMatch) {
      events.push({ name: colonMatch[1], description: colonMatch[2].trim() });
      continue;
    }
    // Format: event_name - description
    const dashMatch = trimmed.match(/^([a-z_][a-z0-9_]*)\s+-\s+(.+)$/i);
    if (dashMatch) {
      events.push({ name: dashMatch[1], description: dashMatch[2].trim() });
      continue;
    }
    // Just an event name
    if (/^[a-z_][a-z0-9_]*$/.test(trimmed)) {
      events.push({ name: trimmed, description: "" });
    }
  }
  return events;
}

// ─── Experiment workflow ───

function cardHasExperimentTag(card) {
  // Favro card.tags is array of tag objects: [{tagId, name, color}, ...]
  // Also try card.customFields or other locations
  const tags = card.tags || card.customFields?.tags || [];
  return tags.some((t) => (t.name || t).toLowerCase() === EXPERIMENT_TAG);
}

async function handleExperimentTagAdded(card, now) {
  const { cardCommonId, sequentialId, name } = card;
  console.log(`🧪 Experiment tag detected on #${sequentialId} "${name}"`);

  experiments[cardCommonId] = {
    cardCommonId,
    cardName: name,
    sequentialId,
    tagDetectedAt: now,
    slackThreadTs: null,
    suggestedEvents: [],
    eventsApproved: false,
    favroCommentPosted: false,
    codeReviewAt: null,
    ga4CheckDone: false,
    ga4CheckResult: null,
    releasedAt: null,
    reportScheduledFor: null,
    reportDone: false,
  };

  const msg =
    `🧪 *Эксперимент: #${sequentialId} ${name}*\n` +
    `Задача помечена тегом \`experiment\`. Нужна помощь с аналитикой.\n\n` +
    `Если есть вопросы по задаче — пишите в тред. ` +
    `Если вопросов нет, предложите список событий в формате:\n` +
    `\`event_name: описание\`\n` +
    `_(напишите \`approve\` когда список готов к записи в Favro)_`;

  const slackRes = await postToSlack(msg);
  if (slackRes?.ts) {
    experiments[cardCommonId].slackThreadTs = slackRes.ts;
  }
}

async function handleCodeReview(card, now) {
  const { cardCommonId, sequentialId, name } = card;
  const exp = experiments[cardCommonId];
  if (!exp || exp.ga4CheckDone) return;

  console.log(`🔍 Code Review — checking GA4 for #${sequentialId} "${name}"`);
  exp.codeReviewAt = now;

  const eventNames = exp.suggestedEvents.map((e) => e.name);

  if (eventNames.length === 0) {
    const msg =
      `🔍 *#${sequentialId} ${name}* перешла в Code Review\n` +
      `Список событий не был добавлен в Favro — нет событий для проверки в GA4.`;
    const threadTs = exp.slackThreadTs;
    await postToSlack(msg, threadTs);
    return;
  }

  // Check GA4
  const ga4Results = await getGA4EventCounts(eventNames, 3);
  exp.ga4CheckDone = true;
  exp.ga4CheckResult = ga4Results;

  let report;
  if (!ga4Results) {
    report =
      `🔍 *#${sequentialId} ${name}* — Code Review\n` +
      `GA4 проверить не удалось (нет доступа к API). Проверьте вручную:\n` +
      eventNames.map((e) => `• \`${e}\``).join("\n");
  } else {
    const lines = eventNames.map((e) => {
      const data = ga4Results[e];
      if (!data || data.count === 0) {
        return `• \`${e}\` — ❌ *не найдено* (за 3 дня)`;
      }
      return `• \`${e}\` — ✅ ${data.count.toLocaleString()} событий, ${data.users.toLocaleString()} юзеров`;
    });
    const allFiring = eventNames.every((e) => ga4Results[e]?.count > 0);
    report =
      `${allFiring ? "✅" : "⚠️"} *#${sequentialId} ${name}* — Code Review, GA4 check\n` +
      lines.join("\n");
    if (!allFiring) {
      report += `\n\n⚠️ Не все события фиксируются — проверьте интеграцию!`;
    }
  }

  await postToSlack(report, exp.slackThreadTs);
}

async function handleReleased(card, now) {
  const { cardCommonId, sequentialId, name } = card;
  const exp = experiments[cardCommonId];
  if (!exp || exp.releasedAt) return;

  console.log(`🚀 Released — scheduling 10-day report for #${sequentialId} "${name}"`);
  exp.releasedAt = now;
  const reportDate = new Date(new Date(now).getTime() + 10 * 24 * 60 * 60 * 1000);
  exp.reportScheduledFor = reportDate.toISOString();
  exp.reportDone = false;

  const msg =
    `🚀 *#${sequentialId} ${name}* — задача релизнута!\n` +
    `Через 10 дней (${reportDate.toLocaleDateString("ru-RU")}) подготовлю GA4-отчёт по результатам эксперимента.`;
  await postToSlack(msg, exp.slackThreadTs);
}

async function runScheduledReports() {
  const now = new Date();
  for (const [cardCommonId, exp] of Object.entries(experiments)) {
    if (exp.reportDone || !exp.reportScheduledFor) continue;
    const scheduledFor = new Date(exp.reportScheduledFor);
    if (now < scheduledFor) continue;

    console.log(`📊 Running 10-day report for #${exp.sequentialId} "${exp.cardName}"`);
    exp.reportDone = true; // mark early to avoid double execution

    const eventNames = exp.suggestedEvents.map((e) => e.name);
    const ga4Results = eventNames.length > 0 ? await getGA4EventCounts(eventNames, 10) : null;

    // Build report text
    let reportText = `📊 *Результаты эксперимента: #${exp.sequentialId} ${exp.cardName}*\n`;
    reportText += `_Прошло 10 дней после релиза_\n\n`;

    if (eventNames.length === 0) {
      reportText += `Список аналитических событий не был задан — данных нет.`;
    } else if (!ga4Results) {
      reportText += `Событий запрошено: ${eventNames.length}\n`;
      reportText += `⚠️ Не удалось получить данные из GA4 (нет доступа к API).\n`;
      reportText += `Проверьте вручную в GA4 → Events:\n`;
      reportText += eventNames.map((e) => `• \`${e}\``).join("\n");
    } else {
      for (const e of eventNames) {
        const d = ga4Results[e];
        if (!d || d.count === 0) {
          reportText += `• \`${e}\` — ❌ нет данных\n`;
        } else {
          reportText += `• \`${e}\` — ${d.count.toLocaleString()} событий, ${d.users.toLocaleString()} юзеров\n`;
        }
      }
    }

    // Post to Slack
    const slackMsg = `${reportText}\n_Отчёт по аналитике за 10 дней после релиза готов._`;
    await postToSlack(slackMsg, exp.slackThreadTs);

    // Add comment to Favro card
    const favroComment =
      `## 📊 Результаты эксперимента (10 дней)\n\n` +
      (eventNames.length === 0
        ? "Аналитические события не были заданы."
        : eventNames
            .map((e) => {
              const d = ga4Results?.[e];
              if (!d || d.count === 0) return `- \`${e}\`: ❌ нет данных`;
              return `- \`${e}\`: ${d.count.toLocaleString()} событий, ${d.users.toLocaleString()} юзеров`;
            })
            .join("\n")) +
      `\n\n_Отчёт сформирован автоматически ${now.toLocaleDateString("ru-RU")}_`;

    await addFavroComment(cardCommonId, favroComment);

    // Save updated state
    saveStateLocal();
    scheduleSaveToGitHub();
  }
}

// Run scheduler every hour
setInterval(runScheduledReports, 60 * 60 * 1000);

// ─── Slack Events API endpoint ───
// Handles messages from users in the analytics channel (to capture event lists)
app.post("/slack-events", async (req, res) => {
  const body = req.body;

  // Slack URL verification challenge
  if (body.type === "url_verification") {
    return res.json({ challenge: body.challenge });
  }

  // Message events
  if (body.type === "event_callback") {
    const event = body.event;

    // Ignore bot messages
    if (event.bot_id || event.subtype === "bot_message") {
      return res.status(200).json({ ok: true });
    }

    // Only handle messages in our channel
    if (event.channel !== SLACK_CHANNEL_ID) {
      return res.status(200).json({ ok: true });
    }

    // Check if this is a reply in an experiment thread
    if (event.type === "message" && event.thread_ts) {
      await handleSlackThreadReply(event);
    }
  }

  res.status(200).json({ ok: true });
});

async function handleSlackThreadReply(event) {
  const { text, thread_ts, user } = event;
  if (!text) return;

  // Find experiment by thread_ts
  const exp = Object.values(experiments).find((e) => e.slackThreadTs === thread_ts);
  if (!exp) return;

  console.log(`💬 Slack reply in experiment thread #${exp.sequentialId}: "${text.slice(0, 80)}"`);

  const lowerText = text.toLowerCase().trim();

  // "approve" — write current events list to Favro
  if (lowerText === "approve" || lowerText === "ок" || lowerText === "ok") {
    if (exp.suggestedEvents.length === 0) {
      await postToSlack(`⚠️ Нет событий для записи. Сначала добавьте список событий.`, thread_ts);
      return;
    }
    const comment = formatEventsTable(exp.suggestedEvents);
    await addFavroComment(exp.cardCommonId, comment);
    exp.eventsApproved = true;
    exp.favroCommentPosted = true;
    await postToSlack(
      `✅ Записала ${exp.suggestedEvents.length} событий в Favro.\nБуду проверять GA4, когда задача уйдёт в Code Review.`,
      thread_ts
    );
    saveStateLocal();
    scheduleSaveToGitHub();
    return;
  }

  // Parse events from message
  const parsed = parseEventsFromText(text);
  if (parsed.length > 0) {
    // Merge with existing (replace if same name)
    for (const newEvent of parsed) {
      const idx = exp.suggestedEvents.findIndex((e) => e.name === newEvent.name);
      if (idx >= 0) {
        exp.suggestedEvents[idx] = newEvent;
      } else {
        exp.suggestedEvents.push(newEvent);
      }
    }
    const list = exp.suggestedEvents.map((e) => `• \`${e.name}\` — ${e.description || "без описания"}`).join("\n");
    await postToSlack(
      `Добавила ${parsed.length} событий. Всего: ${exp.suggestedEvents.length}\n\n${list}\n\nЕсли список готов — напишите \`approve\``,
      thread_ts
    );
    saveStateLocal();
    scheduleSaveToGitHub();
  }
}

// ─── Webhook endpoint ───
app.post("/favro-webhook", async (req, res) => {
  const body = req.body;
  const now = new Date().toISOString();

  console.log("=== FAVRO EVENT ===", now);

  if (body.test) {
    console.log("Test ping received");
    return res.status(200).json({ ok: true, message: "pong" });
  }

  const card = body.card;
  if (!card) {
    console.log("No card data in payload");
    return res.status(200).json({ ok: true, message: "no card data" });
  }

  const cardCommonId = card.cardCommonId;
  const currentColId = card.columnId;
  const currentColName = colName(currentColId);
  const assignees = (card.assignments || []).map((a) => userName(a.userId));

  const event = {
    timestamp: now,
    cardCommonId,
    cardId: card.cardId,
    sequentialId: card.sequentialId,
    name: card.name,
    columnId: currentColId,
    columnName: currentColName,
    assignees,
    timeOnColumns: card.timeOnColumns || {},
  };

  const prev = cardState[cardCommonId];

  // ─── Experiment tag detection ───
  const hasExperimentTag = cardHasExperimentTag(card);
  const wasTracked = !!experiments[cardCommonId];

  if (hasExperimentTag && !wasTracked) {
    // New experiment card — fire and forget (async)
    handleExperimentTagAdded(card, now).catch(console.error);
  }

  // ─── Column change tracking ───
  if (prev && prev.columnId !== currentColId) {
    const fromName = prev.columnName;
    const toName = currentColName;
    const moveKey = `${fromName} -> ${toName}`;

    event.previousColumn = fromName;
    event.moveDirection =
      colOrder(currentColId) < colOrder(prev.columnId) ? "backward" : "forward";

    if (SIGNIFICANT_RETURNS.has(moveKey)) {
      const returnEvent = {
        timestamp: now,
        cardCommonId,
        sequentialId: card.sequentialId,
        cardName: card.name,
        fromColumn: fromName,
        toColumn: toName,
        returnType: classifyReturn(fromName),
        assignees,
        timeOnColumns: card.timeOnColumns || {},
      };
      returns.push(returnEvent);
      event.isReturn = true;
      event.returnType = returnEvent.returnType;
      console.log(
        `⚠ RETURN: #${card.sequentialId} "${card.name}" | ${moveKey} | ${returnEvent.returnType}`
      );
    } else {
      console.log(`→ Move: #${card.sequentialId} "${card.name}" | ${moveKey} (${event.moveDirection})`);
    }

    // ─── Experiment workflow triggers on column change ───
    if (hasExperimentTag || wasTracked) {
      if (toName === "Code Review") {
        handleCodeReview(card, now).catch(console.error);
      }
      if (toName === "Released" || toName === "Done") {
        handleReleased(card, now).catch(console.error);
      }
    }
  } else if (!prev) {
    console.log(`+ New card: #${card.sequentialId} "${card.name}" in ${currentColName}`);
  } else {
    console.log(`= Same col: #${card.sequentialId} "${card.name}" in ${currentColName}`);
  }

  cardState[cardCommonId] = {
    columnId: currentColId,
    columnName: currentColName,
    lastSeen: now,
    sequentialId: card.sequentialId,
    name: card.name,
    assignees,
  };

  allEvents.push(event);
  saveStateLocal();
  scheduleSaveToGitHub();

  res.status(200).json({ ok: true });
});

// ─── API endpoints ───

app.get("/", (req, res) => res.send("Favro webhook logger + Experiment bot is running"));

app.get("/api/returns", (req, res) => {
  res.json({ total: returns.length, returns: returns.slice().reverse() });
});

app.get("/api/returns/stats", (req, res) => {
  const byType = {}, byAssignee = {}, byCard = {};
  for (const r of returns) {
    byType[r.returnType] = (byType[r.returnType] || 0) + 1;
    for (const a of r.assignees) byAssignee[a] = (byAssignee[a] || 0) + 1;
    const key = `#${r.sequentialId} ${r.cardName}`;
    byCard[key] = (byCard[key] || 0) + 1;
  }
  const sortObj = (obj) =>
    Object.fromEntries(Object.entries(obj).sort((a, b) => b[1] - a[1]));
  res.json({
    totalReturns: returns.length,
    byType: sortObj(byType),
    byAssignee: sortObj(byAssignee),
    byCard: sortObj(byCard),
    trackedCards: Object.keys(cardState).length,
    totalEvents: allEvents.length,
  });
});

app.get("/api/events", (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  res.json({ total: allEvents.length, events: allEvents.slice(-limit).reverse() });
});

app.get("/api/state", (req, res) => {
  res.json({ trackedCards: Object.keys(cardState).length, cards: cardState });
});

app.get("/api/experiments", (req, res) => {
  res.json({
    total: Object.keys(experiments).length,
    experiments: Object.values(experiments).sort(
      (a, b) => new Date(b.tagDetectedAt) - new Date(a.tagDetectedAt)
    ),
  });
});

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    githubPersistence: !!(GH_TOKEN && GH_REPO),
    slackConfigured: !!(SLACK_BOT_TOKEN && SLACK_CHANNEL_ID),
    favroConfigured: !!(FAVRO_EMAIL && FAVRO_API_TOKEN && FAVRO_ORG_ID),
    ga4Configured: !!GA4_SERVICE_ACCOUNT_JSON,
    uptime: process.uptime(),
    trackedCards: Object.keys(cardState).length,
    totalEvents: allEvents.length,
    totalReturns: returns.length,
    activeExperiments: Object.values(experiments).filter((e) => !e.reportDone).length,
  });
});

app.post("/api/save", async (req, res) => {
  await saveToGitHub();
  res.json({ ok: true, message: "Saved to GitHub" });
});

// Manual trigger for scheduled reports (for testing)
app.post("/api/run-reports", async (req, res) => {
  await runScheduledReports();
  res.json({ ok: true });
});

// ─── Start ───
async function start() {
  await loadStateFromGitHub();
  // Run scheduler on startup (catches any overdue reports after restarts)
  runScheduledReports().catch(console.error);
  const PORT = process.env.PORT || 10000;
  app.listen(PORT, () => {
    console.log(`Favro webhook logger + Experiment bot listening on port ${PORT}`);
    console.log(`GitHub: ${GH_TOKEN && GH_REPO ? "ENABLED" : "DISABLED"}`);
    console.log(`Slack: ${SLACK_BOT_TOKEN && SLACK_CHANNEL_ID ? "ENABLED" : "DISABLED"}`);
    console.log(`Favro API: ${FAVRO_EMAIL && FAVRO_API_TOKEN ? "ENABLED" : "DISABLED"}`);
    console.log(`GA4: ${GA4_SERVICE_ACCOUNT_JSON ? "ENABLED" : "DISABLED"}`);
  });
}

start();

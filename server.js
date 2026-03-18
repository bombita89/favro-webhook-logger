const express = require("express");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(express.json({ limit: "1mb" }));

// ─── Config ───
const GH_TOKEN = process.env.GH_TOKEN;
const GH_REPO = process.env.GH_REPO;
const GH_BRANCH = process.env.GH_BRANCH || "main";
const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN;
const SLACK_CHANNEL_ID = process.env.SLACK_CHANNEL_ID;
const FAVRO_EMAIL = process.env.FAVRO_EMAIL;
const FAVRO_API_TOKEN = process.env.FAVRO_API_TOKEN;
const FAVRO_ORG_ID = process.env.FAVRO_ORG_ID;
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
    }
    const eventsResult = await ghReadFile("events.json");
    if (eventsResult) {
      allEvents.push(...eventsResult.content);
      fileShas["events.json"] = eventsResult.sha;
    }
    const returnsResult = await ghReadFile("returns.json");
    if (returnsResult) {
      returns.push(...returnsResult.content);
      fileShas["returns.json"] = returnsResult.sha;
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
function scheduleSaveToGitHub() {
  if (!GH_TOKEN || !GH_REPO) return;
  if (saveTimeout) clearTimeout(saveTimeout);
  saveTimeout = setTimeout(() => saveToGitHub(), 5000);
}

async function saveToGitHub() {
  if (!GH_TOKEN || !GH_REPO) return;
  try {
    for (const f of [
      { name: "card_state.json", data: cardState },
      { name: "events.json", data: allEvents },
      { name: "returns.json", data: returns },
      { name: "experiments.json", data: experiments },
    ]) {
      const result = await ghWriteFile(f.name, f.data, fileShas[f.name]);
      if (result?.content) fileShas[f.name] = result.content.sha;
    }
    console.log("GitHub save complete");
  } catch (e) {
    console.error("Error saving to GitHub:", e.message);
  }
}

const DATA_DIR = path.join(__dirname, "data");
function saveStateLocal() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    for (const [name, data] of Object.entries({
      "card_state.json": cardState,
      "events.json": allEvents,
      "returns.json": returns,
      "experiments.json": experiments,
    })) {
      fs.writeFileSync(path.join(DATA_DIR, name), JSON.stringify(data, null, 2));
    }
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

// ─── Slack ───
async function postToSlack(text, threadTs = null) {
  if (!SLACK_BOT_TOKEN || !SLACK_CHANNEL_ID) {
    console.log("[SLACK MOCK]", threadTs ? `(thread ${threadTs})` : "", text.slice(0, 120));
    return { ts: Date.now().toString(), ok: true };
  }
  const body = { channel: SLACK_CHANNEL_ID, text };
  if (threadTs) body.thread_ts = threadTs;
  try {
    const res = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { Authorization: `Bearer ${SLACK_BOT_TOKEN}`, "Content-Type": "application/json" },
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

// ─── Favro API ───
async function addFavroComment(cardCommonId, commentText) {
  if (!FAVRO_EMAIL || !FAVRO_API_TOKEN || !FAVRO_ORG_ID) {
    console.log("[FAVRO MOCK] Comment on", cardCommonId, ":", commentText.slice(0, 80));
    return { ok: true };
  }
  try {
    const creds = Buffer.from(`${FAVRO_EMAIL}:${FAVRO_API_TOKEN}`).toString("base64");
    const res = await fetch("https://favro.com/api/1.0/comments", {
      method: "POST",
      headers: {
        Authorization: `Basic ${creds}`,
        organizationId: FAVRO_ORG_ID,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ cardCommonId, comment: commentText }),
    });
    const data = await res.json();
    if (data.errors) console.error("Favro comment error:", JSON.stringify(data.errors));
    return data;
  } catch (e) {
    console.error("Favro request failed:", e.message);
    return null;
  }
}

// ─── Format events table for Favro ───
function formatEventsTable(events) {
  const rows = events.map((e) => `| \`${e.name}\` | ${e.description || "—"} | ⬜ Planned |`);
  return ["**Analytics Events**", "", "| Event | Description | Status |", "|---|---|---|", ...rows].join("\n");
}

// ─── Parse events from Slack message ───
function parseEventsFromText(text) {
  const events = [];
  for (const line of text.split("\n")) {
    const trimmed = line.trim().replace(/^[-*•]\s*/, "");
    if (!trimmed) continue;
    const colonMatch = trimmed.match(/^([a-z_][a-z0-9_]*)\s*:\s*(.+)$/i);
    if (colonMatch) { events.push({ name: colonMatch[1], description: colonMatch[2].trim() }); continue; }
    const dashMatch = trimmed.match(/^([a-z_][a-z0-9_]*)\s+-\s+(.+)$/i);
    if (dashMatch) { events.push({ name: dashMatch[1], description: dashMatch[2].trim() }); continue; }
    if (/^[a-z_][a-z0-9_]*$/.test(trimmed)) events.push({ name: trimmed, description: "" });
  }
  return events;
}

// ─── Check if card has experiment tag ───
function cardHasExperimentTag(card) {
  const tags = card.tags || [];
  return tags.some((t) => (t.name || t).toLowerCase() === EXPERIMENT_TAG);
}

// ─── Experiment workflow handlers ───

async function handleExperimentTagAdded(card, now) {
  console.log(`🧪 Experiment: #${card.sequentialId} "${card.name}"`);
  experiments[card.cardCommonId] = {
    cardCommonId: card.cardCommonId,
    cardName: card.name,
    sequentialId: card.sequentialId,
    tagDetectedAt: now,
    slackThreadTs: null,
    suggestedEvents: [],
    eventsApproved: false,
    favroCommentPosted: false,
    codeReviewAt: null,
    ga4CheckNeeded: false,
    ga4CheckDone: false,
    releasedAt: null,
    reportScheduledFor: null,
    reportDone: false,
  };

  const msg =
    `🧪 *Эксперимент: #${card.sequentialId} ${card.name}*\n` +
    `Задача помечена \`experiment\`. Нужна помощь с аналитикой.\n\n` +
    `Если есть вопросы по задаче — пишите в тред.\n` +
    `Если вопросов нет — предложите список событий в формате:\n` +
    `\`event_name: описание\`\n` +
    `_Когда список готов → напишите \`approve\` чтобы записать в Favro._`;

  const res = await postToSlack(msg);
  if (res?.ts) experiments[card.cardCommonId].slackThreadTs = res.ts;
}

async function handleCodeReview(card, now) {
  const exp = experiments[card.cardCommonId];
  if (!exp || exp.codeReviewAt) return;
  console.log(`🔍 Code Review — marking GA4 check needed for #${card.sequentialId}`);

  exp.codeReviewAt = now;
  exp.ga4CheckNeeded = true;
  exp.ga4CheckDone = false;

  const eventCount = exp.suggestedEvents.length;
  const msg = eventCount > 0
    ? `🔍 *#${card.sequentialId} ${card.name}* — Code Review\n` +
      `Проверю GA4 (${eventCount} событий) — результат будет скоро.`
    : `🔍 *#${card.sequentialId} ${card.name}* — Code Review\n` +
      `⚠️ Список событий не задан — нечего проверять в GA4.`;

  await postToSlack(msg, exp.slackThreadTs);
}

async function handleReleased(card, now) {
  const exp = experiments[card.cardCommonId];
  if (!exp || exp.releasedAt) return;
  console.log(`🚀 Released: #${card.sequentialId} — scheduling 10-day report`);

  exp.releasedAt = now;
  const reportDate = new Date(new Date(now).getTime() + 10 * 24 * 60 * 60 * 1000);
  exp.reportScheduledFor = reportDate.toISOString();

  await postToSlack(
    `🚀 *#${card.sequentialId} ${card.name}* — релиз!\n` +
    `Через 10 дней (${reportDate.toLocaleDateString("ru-RU")}) подготовлю GA4-отчёт по результатам.`,
    exp.slackThreadTs
  );
}

// ─── Slack Events API endpoint ───
app.post("/slack-events", async (req, res) => {
  const body = req.body;
  if (body.type === "url_verification") return res.json({ challenge: body.challenge });

  if (body.type === "event_callback") {
    const event = body.event;
    if (event.bot_id || event.subtype === "bot_message") return res.status(200).json({ ok: true });
    if (event.channel !== SLACK_CHANNEL_ID) return res.status(200).json({ ok: true });
    if (event.type === "message" && event.thread_ts) {
      handleSlackThreadReply(event).catch(console.error);
    }
  }
  res.status(200).json({ ok: true });
});

async function handleSlackThreadReply(event) {
  const { text, thread_ts } = event;
  if (!text) return;

  const exp = Object.values(experiments).find((e) => e.slackThreadTs === thread_ts);
  if (!exp) return;

  console.log(`💬 Slack reply for #${exp.sequentialId}: "${text.slice(0, 60)}"`);
  const lower = text.toLowerCase().trim();

  if (lower === "approve" || lower === "ок" || lower === "ok" || lower === "окей") {
    if (exp.suggestedEvents.length === 0) {
      await postToSlack(`⚠️ Нет событий для записи. Сначала добавьте список.`, thread_ts);
      return;
    }
    await addFavroComment(exp.cardCommonId, formatEventsTable(exp.suggestedEvents));
    exp.eventsApproved = true;
    exp.favroCommentPosted = true;
    await postToSlack(
      `✅ Записала ${exp.suggestedEvents.length} событий в Favro.\n` +
      `Буду проверять GA4 когда задача уйдёт в Code Review.`,
      thread_ts
    );
    saveStateLocal();
    scheduleSaveToGitHub();
    return;
  }

  const parsed = parseEventsFromText(text);
  if (parsed.length > 0) {
    for (const e of parsed) {
      const idx = exp.suggestedEvents.findIndex((x) => x.name === e.name);
      if (idx >= 0) exp.suggestedEvents[idx] = e;
      else exp.suggestedEvents.push(e);
    }
    const list = exp.suggestedEvents
      .map((e) => `• \`${e.name}\` — ${e.description || "без описания"}`)
      .join("\n");
    await postToSlack(
      `Добавила ${parsed.length} событий. Всего: ${exp.suggestedEvents.length}\n\n${list}\n\n` +
      `Когда готово → напишите \`approve\``,
      thread_ts
    );
    saveStateLocal();
    scheduleSaveToGitHub();
  }
}

// ─── Favro webhook ───
app.post("/favro-webhook", async (req, res) => {
  const body = req.body;
  const now = new Date().toISOString();

  if (body.test) return res.status(200).json({ ok: true, message: "pong" });
  const card = body.card;
  if (!card) return res.status(200).json({ ok: true });

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
  const hasExpTag = cardHasExperimentTag(card);
  const isTrackedExperiment = !!experiments[cardCommonId];

  // Detect new experiment tag
  if (hasExpTag && !isTrackedExperiment) {
    handleExperimentTagAdded(card, now).catch(console.error);
  }

  // Column change logic
  if (prev && prev.columnId !== currentColId) {
    const fromName = prev.columnName;
    const toName = currentColName;
    const moveKey = `${fromName} -> ${toName}`;

    event.previousColumn = fromName;
    event.moveDirection = colOrder(currentColId) < colOrder(prev.columnId) ? "backward" : "forward";

    if (SIGNIFICANT_RETURNS.has(moveKey)) {
      const ret = { timestamp: now, cardCommonId, sequentialId: card.sequentialId,
        cardName: card.name, fromColumn: fromName, toColumn: toName,
        returnType: classifyReturn(fromName), assignees, timeOnColumns: card.timeOnColumns || {} };
      returns.push(ret);
      event.isReturn = true;
      event.returnType = ret.returnType;
      console.log(`⚠ RETURN: #${card.sequentialId} "${card.name}" | ${moveKey}`);
    } else {
      console.log(`→ Move: #${card.sequentialId} "${card.name}" | ${moveKey}`);
    }

    // Experiment workflow
    if (hasExpTag || isTrackedExperiment) {
      if (toName === "Code Review") handleCodeReview(card, now).catch(console.error);
      if (toName === "Released" || toName === "Done") handleReleased(card, now).catch(console.error);
    }
  } else if (!prev) {
    console.log(`+ New: #${card.sequentialId} "${card.name}" in ${currentColName}`);
  }

  cardState[cardCommonId] = { columnId: currentColId, columnName: currentColName,
    lastSeen: now, sequentialId: card.sequentialId, name: card.name, assignees };

  allEvents.push(event);
  saveStateLocal();
  scheduleSaveToGitHub();

  res.status(200).json({ ok: true });
});

// ─── API ───
app.get("/", (req, res) => res.send("Favro webhook logger + Experiment bot"));

app.get("/api/returns", (req, res) =>
  res.json({ total: returns.length, returns: returns.slice().reverse() }));

app.get("/api/returns/stats", (req, res) => {
  const byType = {}, byAssignee = {}, byCard = {};
  for (const r of returns) {
    byType[r.returnType] = (byType[r.returnType] || 0) + 1;
    for (const a of r.assignees) byAssignee[a] = (byAssignee[a] || 0) + 1;
    const key = `#${r.sequentialId} ${r.cardName}`;
    byCard[key] = (byCard[key] || 0) + 1;
  }
  const sort = (o) => Object.fromEntries(Object.entries(o).sort((a, b) => b[1] - a[1]));
  res.json({ totalReturns: returns.length, byType: sort(byType),
    byAssignee: sort(byAssignee), byCard: sort(byCard),
    trackedCards: Object.keys(cardState).length, totalEvents: allEvents.length });
});

app.get("/api/events", (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  res.json({ total: allEvents.length, events: allEvents.slice(-limit).reverse() });
});

app.get("/api/state", (req, res) =>
  res.json({ trackedCards: Object.keys(cardState).length, cards: cardState }));

app.get("/api/experiments", (req, res) =>
  res.json({ total: Object.keys(experiments).length,
    experiments: Object.values(experiments).sort(
      (a, b) => new Date(b.tagDetectedAt) - new Date(a.tagDetectedAt)) }));

app.get("/health", (req, res) =>
  res.json({ status: "ok",
    github: !!(GH_TOKEN && GH_REPO),
    slack: !!(SLACK_BOT_TOKEN && SLACK_CHANNEL_ID),
    favro: !!(FAVRO_EMAIL && FAVRO_API_TOKEN && FAVRO_ORG_ID),
    uptime: process.uptime(),
    trackedCards: Object.keys(cardState).length,
    totalEvents: allEvents.length,
    totalReturns: returns.length,
    activeExperiments: Object.values(experiments).filter((e) => !e.reportDone).length }));

app.post("/api/save", async (req, res) => {
  await saveToGitHub();
  res.json({ ok: true });
});

// ─── Slack polling (check thread replies every 60s, no Events API needed) ───
const lastSeenTs = {}; // cardCommonId → last processed message ts

async function pollSlackThreads() {
  const activeExperiments = Object.values(experiments).filter(
    (e) => e.slackThreadTs && !e.reportDone && !e.eventsApproved
  );
  if (activeExperiments.length === 0) return;

  for (const exp of activeExperiments) {
    try {
      const oldest = lastSeenTs[exp.cardCommonId] || exp.slackThreadTs;
      const url = `https://slack.com/api/conversations.replies?channel=${SLACK_CHANNEL_ID}&ts=${exp.slackThreadTs}&oldest=${oldest}&limit=20`;
      const res = await fetch(url, {
        headers: { Authorization: `Bearer ${SLACK_BOT_TOKEN}` },
      });
      const data = await res.json();
      if (!data.ok) { console.error("Slack poll error:", data.error); continue; }

      const messages = (data.messages || []).filter(
        (m) => m.ts !== exp.slackThreadTs && m.ts > oldest && !m.bot_id
      );

      for (const msg of messages) {
        await handleSlackThreadReply({ text: msg.text, thread_ts: exp.slackThreadTs, user: msg.user });
        lastSeenTs[exp.cardCommonId] = msg.ts;
      }
    } catch (e) {
      console.error(`Poll error for #${exp.sequentialId}:`, e.message);
    }
  }
}

// ─── Start ───
async function start() {
  await loadStateFromGitHub();
  if (SLACK_BOT_TOKEN && SLACK_CHANNEL_ID) {
    setInterval(pollSlackThreads, 60 * 1000);
    console.log("Slack polling: ON (60s)");
  }
  const PORT = process.env.PORT || 10000;
  app.listen(PORT, () => {
    console.log(`Server listening on :${PORT}`);
    console.log(`GitHub: ${GH_TOKEN && GH_REPO ? "ON" : "OFF"} | Slack: ${SLACK_BOT_TOKEN ? "ON" : "OFF"} | Favro: ${FAVRO_EMAIL ? "ON" : "OFF"}`);
  });
}

start();

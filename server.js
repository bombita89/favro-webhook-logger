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
  "Code Review -> Doing", "Code Review -> Ready For Dev",
  "Waits for Build -> Doing", "Waits for Build -> Ready For Dev",
  "QA Todo -> Doing", "QA Todo -> Ready For Dev",
  "QA Doing -> Doing", "QA Doing -> Ready For Dev", "QA Doing -> QA Todo",
  "Ready for Release -> Doing", "Ready for Release -> Ready For Dev",
  "Ready for Release -> QA Todo", "Ready for Release -> QA Doing",
]);

// ─── In-memory state ───
const cardState = {};
const allEvents = [];
const returns = [];
const experiments = {};

// ─── GitHub API helpers ───
async function ghApiRequest(method, filePath, body = null) {
  if (!GH_TOKEN || !GH_REPO) return null;
  // FIX: always include ?ref=GH_BRANCH for GET so we read from the correct branch
  const base = `https://api.github.com/repos/${GH_REPO}/contents/data/${filePath}`;
  const url = method === "GET" ? `${base}?ref=${GH_BRANCH}` : base;
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
    const errText = await res.text();
    console.error(`GitHub API error ${res.status} for ${filePath}:`, errText);
    // Return status so callers can handle 409
    return { __error: res.status, __body: errText };
  }
  if (res.status === 404) return null;
  return res.json();
}

async function ghReadFile(filePath) {
  const data = await ghApiRequest("GET", filePath);
  if (!data || data.__error || !data.content) return null;
  const content = Buffer.from(data.content, "base64").toString("utf8");
  return { content: JSON.parse(content), sha: data.sha };
}

async function ghGetCurrentSha(filePath) {
  const data = await ghApiRequest("GET", filePath);
  if (!data || data.__error) return null;
  return data.sha || null;
}

async function ghWriteFile(filePath, jsonData, sha) {
  const content = Buffer.from(JSON.stringify(jsonData, null, 2)).toString("base64");
  const body = {
    message: `auto: update ${filePath} [${new Date().toISOString()}]`,
    content,
    branch: GH_BRANCH,
  };
  if (sha) body.sha = sha;
  const result = await ghApiRequest("PUT", filePath, body);

  // FIX: on 409 (SHA conflict), fetch fresh SHA and retry once
  if (result && result.__error === 409) {
    console.log(`SHA conflict for ${filePath}, fetching fresh SHA and retrying...`);
    const freshSha = await ghGetCurrentSha(filePath);
    if (freshSha) {
      body.sha = freshSha;
      const retry = await ghApiRequest("PUT", filePath, body);
      if (retry && !retry.__error) {
        fileShas[filePath] = retry.content?.sha || freshSha;
        console.log(`Retry succeeded for ${filePath}`);
        return retry;
      }
    }
    return null;
  }
  return result;
}

const fileShas = {};
let propertyMapping = {};
let measurementIdMap = {};

async function loadStateFromGitHub() {
  if (!GH_TOKEN || !GH_REPO) {
    console.log("⚠ GitHub persistence disabled");
    return;
  }
  console.log(`Loading state from GitHub (${GH_REPO}, branch: ${GH_BRANCH})...`);
  try {
    for (const [key, target] of [
      ["card_state.json", cardState],
      ["experiments.json", experiments],
    ]) {
      const r = await ghReadFile(key);
      if (r) { Object.assign(target, r.content); fileShas[key] = r.sha; }
    }
    const evR = await ghReadFile("events.json");
    if (evR) { allEvents.push(...evR.content); fileShas["events.json"] = evR.sha; }
    const retR = await ghReadFile("returns.json");
    if (retR) { returns.push(...retR.content); fileShas["returns.json"] = retR.sha; }
    const mapR = await ghReadFile("property_mapping.json");
    if (mapR) { Object.assign(propertyMapping, mapR.content); fileShas["property_mapping.json"] = mapR.sha; }
    const midR = await ghReadFile("measurement_id_map.json");
    if (midR) { Object.assign(measurementIdMap, midR.content); fileShas["measurement_id_map.json"] = midR.sha; }

    // FIX: normalize loaded experiments — ensure all required fields exist
    // (handles experiments created by older server versions)
    for (const [id, exp] of Object.entries(experiments)) {
      if (exp.awaitingProperty === undefined) {
        exp.awaitingProperty = !exp.ga4PropertyId;
      }
      if (exp.needsAnalysis === undefined) exp.needsAnalysis = false;
      if (exp.analysisPosted === undefined) exp.analysisPosted = false;
      if (exp.suggestedEvents === undefined) exp.suggestedEvents = [];
      if (exp.eventsApproved === undefined) exp.eventsApproved = false;
      if (exp.favroCommentPosted === undefined) exp.favroCommentPosted = false;
      if (exp.testCheckNeeded === undefined) exp.testCheckNeeded = false;
      if (exp.testCheckDone === undefined) exp.testCheckDone = false;
      if (exp.reportDone === undefined) exp.reportDone = false;
    }

    console.log(`Loaded: ${Object.keys(cardState).length} cards, ${Object.keys(experiments).length} experiments, ${Object.keys(propertyMapping).length} property mappings`);
    // Print experiment states for debugging
    for (const exp of Object.values(experiments)) {
      console.log(`  Exp #${exp.sequentialId} "${exp.cardName}": awaitingProperty=${exp.awaitingProperty}, ga4PropertyId=${exp.ga4PropertyId}, slackThreadTs=${exp.slackThreadTs}`);
    }
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
      { name: "property_mapping.json", data: propertyMapping },
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
      "card_state.json": cardState, "events.json": allEvents,
      "returns.json": returns, "experiments.json": experiments,
      "property_mapping.json": propertyMapping,
    })) {
      fs.writeFileSync(path.join(DATA_DIR, name), JSON.stringify(data, null, 2));
    }
  } catch (e) { console.error("Local save failed:", e.message); }
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

function extractProjectFromName(cardName) {
  const match = (cardName || "").match(/^\[([^\]]+)\]/);
  return match ? match[1].trim().toLowerCase() : null;
}

function extractTestUrls(text) {
  if (!text) return [];
  const regex = /TEST(?:\s+\w+)?:\s*(https?:\/\/[^\s\n]+)/gi;
  const urls = [];
  let m;
  while ((m = regex.exec(text)) !== null) urls.push(m[1]);
  return urls;
}

const ANALYTICS_PLACEHOLDER = "Часть, заполняемая аналитиком";
function parseDescriptionSections(description) {
  if (!description) return { analyticsContent: null, isDefault: true, monitoringContent: null };
  const analyticsMatch = description.match(/\*\*Аналитика\*\*\s*\n([\s\S]*?)(?=\n\*\*|$)/i);
  const analyticsContent = analyticsMatch ? analyticsMatch[1].trim() : null;
  const isDefault = !analyticsContent || analyticsContent.includes(ANALYTICS_PLACEHOLDER);
  const monitoringMatch = description.match(/\*\*Мониторинг и QA\*\*\s*\n([\s\S]*?)(?=\n\*\*|$)/i);
  const monitoringContent = monitoringMatch ? monitoringMatch[1].trim() : null;
  return { analyticsContent, isDefault, monitoringContent };
}

// ─── Favro API ───
function favroHeaders() {
  const creds = Buffer.from(`${FAVRO_EMAIL}:${FAVRO_API_TOKEN}`).toString("base64");
  return {
    Authorization: `Basic ${creds}`,
    organizationId: FAVRO_ORG_ID,
    "Content-Type": "application/json",
  };
}

async function fetchFavroCard(cardCommonId) {
  if (!FAVRO_EMAIL || !FAVRO_API_TOKEN || !FAVRO_ORG_ID) return null;
  try {
    const res = await fetch(
      `https://favro.com/api/v1/cards?cardCommonId=${cardCommonId}`,
      { headers: favroHeaders() }
    );
    const data = await res.json();
    if (data.entities && data.entities.length > 0) return data.entities[0];
    return null;
  } catch (e) {
    console.error("fetchFavroCard error:", e.message);
    return null;
  }
}

async function fetchFavroComments(cardCommonId) {
  if (!FAVRO_EMAIL || !FAVRO_API_TOKEN || !FAVRO_ORG_ID) return [];
  try {
    const res = await fetch(
      `https://favro.com/api/v1/comments?cardCommonId=${cardCommonId}`,
      { headers: favroHeaders() }
    );
    const data = await res.json();
    return data.entities || [];
  } catch (e) {
    console.error("fetchFavroComments error:", e.message);
    return [];
  }
}

async function updateFavroCardDescription(cardId, newDescription) {
  if (!FAVRO_EMAIL || !FAVRO_API_TOKEN || !FAVRO_ORG_ID) {
    console.log("[FAVRO MOCK] Would update card", cardId, "description");
    return true;
  }
  try {
    const res = await fetch(`https://favro.com/api/v1/cards/${cardId}`, {
      method: "PUT",
      headers: favroHeaders(),
      body: JSON.stringify({ description: newDescription }),
    });
    const data = await res.json();
    return !data.errors;
  } catch (e) {
    console.error("updateFavroCardDescription error:", e.message);
    return false;
  }
}

async function addFavroComment(cardCommonId, commentText) {
  if (!FAVRO_EMAIL || !FAVRO_API_TOKEN || !FAVRO_ORG_ID) {
    console.log("[FAVRO MOCK] Comment:", commentText.slice(0, 80));
    return { ok: true };
  }
  try {
    const res = await fetch("https://favro.com/api/v1/comments", {
      method: "POST",
      headers: favroHeaders(),
      body: JSON.stringify({ cardCommonId, comment: commentText }),
    });
    return await res.json();
  } catch (e) {
    console.error("addFavroComment error:", e.message);
    return null;
  }
}

async function writeAnalyticsToFavro(exp, eventsTable) {
  const { cardId, cardCommonId, cardDescription } = exp;
  const { isDefault } = parseDescriptionSections(cardDescription);
  if (isDefault && cardId && cardDescription) {
    const SECTION_REGEX = /(\*\*Аналитика\*\*\s*\n)([\s\S]*?)(?=\n\*\*|$)/i;
    const newDesc = cardDescription.replace(SECTION_REGEX, (_, heading) => {
      return heading + eventsTable + "\n";
    });
    const ok = await updateFavroCardDescription(cardId, newDesc);
    if (ok) { console.log("Updated Аналитика section in card description"); return; }
  }
  await addFavroComment(cardCommonId, "**Analytics Events**\n\n" + eventsTable);
}

// ─── Slack ───
async function postToSlack(text, threadTs = null) {
  if (!SLACK_BOT_TOKEN || !SLACK_CHANNEL_ID) {
    console.log("[SLACK MOCK]", text.slice(0, 100));
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
    if (!data.ok) {
      console.error("Slack postMessage error:", data.error, "| text:", text.slice(0, 60));
    } else {
      console.log(`✉ Slack sent (thread:${threadTs ? "yes" : "no"}): "${text.slice(0, 60)}"`);
    }
    return data;
  } catch (e) {
    console.error("Slack request failed:", e.message);
    return null;
  }
}

// ─── Format events table for Favro ───
function formatEventsTable(events) {
  const rows = events.map((e) => `| \`${e.name}\` | ${e.description || "—"} | ⬜ Planned |`);
  return ["| Event | Description | Status |", "|---|---|---|", ...rows].join("\n");
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
function cardHasExperimentTag(card, req) {
  if (req && req.query.trigger === "experiment-tag") return true;
  const tags = card.tags || [];
  return tags.some((t) => (t.name || t).toLowerCase() === EXPERIMENT_TAG);
}

// ─── Experiment workflow ───

async function handleExperimentTagAdded(card, now) {
  const { cardCommonId, cardId, sequentialId, name } = card;
  console.log(`🧪 Experiment: #${sequentialId} "${name}"`);

  const fullCard = await fetchFavroCard(cardCommonId);
  const comments = await fetchFavroComments(cardCommonId);

  const description = fullCard?.description || "";
  const { monitoringContent } = parseDescriptionSections(description);

  const testUrls = [
    ...extractTestUrls(monitoringContent),
    ...comments.flatMap((c) => extractTestUrls(c.comment)),
  ];

  const projectName = extractProjectFromName(name);
  const ga4PropertyId = projectName ? propertyMapping[projectName] : null;

  experiments[cardCommonId] = {
    cardCommonId,
    cardId: fullCard?.cardId || cardId || null,
    cardName: name,
    sequentialId,
    tagDetectedAt: now,
    projectName,
    ga4PropertyId,
    cardDescription: description,
    testUrls,
    slackThreadTs: null,
    awaitingProperty: !ga4PropertyId,
    needsAnalysis: !!ga4PropertyId,
    analysisPosted: false,
    suggestedEvents: [],
    eventsApproved: false,
    favroCommentPosted: false,
    codeReviewAt: null,
    qaAt: null,
    testCheckNeeded: false,
    testCheckDone: false,
    releasedAt: null,
    reportScheduledFor: null,
    reportDone: false,
  };

  let msg = `🧪 *Эксперимент: #${sequentialId} ${name}*\n`;
  msg += `Задача помечена \`experiment\`. Нужна помощь с аналитикой.\n\n`;

  if (projectName && ga4PropertyId) {
    msg += `Проект: \`${projectName}\` → GA4 property \`${ga4PropertyId}\`\n`;
    msg += `⏳ Анализ запрошен. Результат будет в треде в течение ~10 минут.`;
  } else if (projectName && !ga4PropertyId) {
    msg += `Проект: \`${projectName}\` — счётчик GA4 неизвестен.\n`;
    msg += `📊 Напишите ID счётчика GA4 (формат: *123456789*)`;
  } else {
    msg += `Не могу определить проект из названия задачи (нужен формат \`[Проект] Название\`).\n`;
    msg += `📊 Напишите название проекта и ID счётчика GA4 (пример: *savefrom.net 123456789*)`;
  }

  if (testUrls.length > 0) {
    msg += `\n\n🔗 Тест: ${testUrls.map((u) => `\`${u}\``).join(", ")}`;
  }

  const res = await postToSlack(msg);
  if (res?.ts) experiments[cardCommonId].slackThreadTs = res.ts;
  saveStateLocal();
  scheduleSaveToGitHub();
}

async function handleCodeReview(card, now) {
  const exp = experiments[card.cardCommonId];
  if (!exp || exp.codeReviewAt) return;
  console.log(`🔍 Code Review: #${card.sequentialId}`);

  const comments = await fetchFavroComments(card.cardCommonId);
  const { monitoringContent } = parseDescriptionSections(exp.cardDescription);
  const testUrls = [
    ...extractTestUrls(monitoringContent),
    ...comments.flatMap((c) => extractTestUrls(c.comment)),
  ];
  if (testUrls.length > 0) exp.testUrls = testUrls;

  exp.codeReviewAt = now;
  exp.testCheckNeeded = true;
  exp.testCheckDone = false;

  const hasEvents = exp.suggestedEvents.length > 0;
  const hasTestUrl = exp.testUrls.length > 0;

  let msg = `🔍 *#${card.sequentialId} ${card.name}* → Code Review\n`;
  if (hasEvents && hasTestUrl) {
    msg += `Проверю события на тестовом окружении — результат будет скоро.`;
  } else if (hasEvents && !hasTestUrl) {
    msg += `⚠️ Тестовый URL не найден в карточке. Добавьте \`TEST: https://...\` в Мониторинг и QA.`;
    exp.testCheckNeeded = false;
  } else {
    msg += `⚠️ Список событий не задан — нечего проверять.`;
    exp.testCheckNeeded = false;
  }

  await postToSlack(msg, exp.slackThreadTs);
  saveStateLocal();
  scheduleSaveToGitHub();
}

async function handleQATodo(card, now) {
  const exp = experiments[card.cardCommonId];
  if (!exp || exp.qaAt) return;
  console.log(`🧪 QA Todo: #${card.sequentialId}`);

  exp.qaAt = now;
  if (!exp.testCheckNeeded && exp.suggestedEvents.length > 0) {
    const comments = await fetchFavroComments(card.cardCommonId);
    const { monitoringContent } = parseDescriptionSections(exp.cardDescription);
    const testUrls = [
      ...extractTestUrls(monitoringContent),
      ...comments.flatMap((c) => extractTestUrls(c.comment)),
    ];
    if (testUrls.length > 0) {
      exp.testUrls = testUrls;
      exp.testCheckNeeded = true;
      exp.testCheckDone = false;
      await postToSlack(
        `🧪 *#${card.sequentialId}* → QA Todo. Проверю события на тесте.`,
        exp.slackThreadTs
      );
    }
  }
  saveStateLocal();
  scheduleSaveToGitHub();
}

async function handleReleased(card, now) {
  const exp = experiments[card.cardCommonId];
  if (!exp || exp.releasedAt) return;
  console.log(`🚀 Released: #${card.sequentialId}`);

  exp.releasedAt = now;
  const reportDate = new Date(new Date(now).getTime() + 10 * 24 * 60 * 60 * 1000);
  exp.reportScheduledFor = reportDate.toISOString();

  await postToSlack(
    `🚀 *#${card.sequentialId} ${card.name}* — релиз!\n` +
    `Через 10 дней (${reportDate.toLocaleDateString("ru-RU")}) подготовлю GA4-отчёт.`,
    exp.slackThreadTs
  );
  saveStateLocal();
  scheduleSaveToGitHub();
}

// ─── Slack thread reply handler ───
async function handleSlackThreadReply(event) {
  const { text, thread_ts } = event;
  if (!text) return;

  const exp = Object.values(experiments).find((e) => e.slackThreadTs === thread_ts);
  if (!exp) {
    console.log(`⚠ No experiment found for thread_ts=${thread_ts}`);
    return;
  }

  console.log(`💬 Reply for #${exp.sequentialId}: "${text.slice(0, 80)}" | awaitingProperty=${exp.awaitingProperty}`);
  const lower = text.toLowerCase().trim();

  // ── State: awaiting GA4 property ID ──
  if (exp.awaitingProperty) {
    // Accept both G-XXXXXXXX measurement ID and numeric property ID
    const gIdMatch = text.match(/\b(G-[A-Z0-9]{8,12})\b/i);
    const numMatch = text.match(/(\d{9,12})/);
    let propertyId = null;
    if (gIdMatch) {
      const gId = gIdMatch[1].toUpperCase();
      propertyId = measurementIdMap[gId];
      if (!propertyId) {
        await postToSlack(
          `Measurement ID \`${gId}\` не найден в списке известных счётчиков.\n` +
          `Пришлите числовой Property ID из GA4 → Admin → Property Settings.`,
          thread_ts
        );
        return;
      }
    } else if (numMatch) {
      propertyId = numMatch[1];
    }
    if (propertyId) {
      exp.ga4PropertyId = propertyId;
      exp.awaitingProperty = false;
      exp.needsAnalysis = true;

      // Extract project name if provided alongside property ID (e.g. "savefrom.net 123456789")
      const projectMatch = text.match(/([a-z0-9][-a-z0-9.]+[a-z0-9])\s+\d{9,12}/i);
      if (projectMatch && !exp.projectName) {
        exp.projectName = projectMatch[1].toLowerCase();
      }

      if (exp.projectName) {
        propertyMapping[exp.projectName] = propertyId;
        console.log(`Saved mapping: ${exp.projectName} → ${propertyId}`);
      }

      await postToSlack(
        `✅ Счётчик GA4 \`${propertyId}\` сохранён для \`${exp.projectName || "проекта"}\`.\n` +
        `⏳ Анализ запрошен. Результат будет в треде в течение ~10 минут.`,
        thread_ts
      );
      saveStateLocal();
      scheduleSaveToGitHub();
      return;
    } else {
      await postToSlack(
        `Не нашла ID счётчика. Пришлите \`G-XXXXXXXX\` или числовой Property ID, например: \`123456789\``,
        thread_ts
      );
      return;
    }
  }

  // ── State: approve events list ──
  if (lower === "approve" || lower === "ок" || lower === "ok" || lower === "окей" || lower === "апрув") {
    if (exp.suggestedEvents.length === 0) {
      await postToSlack(`⚠️ Нет событий для записи. Сначала добавьте список.`, thread_ts);
      return;
    }
    const table = formatEventsTable(exp.suggestedEvents);
    await writeAnalyticsToFavro(exp, table);
    exp.eventsApproved = true;
    exp.favroCommentPosted = true;
    await postToSlack(
      `✅ Записала ${exp.suggestedEvents.length} событий в Favro.\n` +
      `Буду проверять на тестовом когда задача уйдёт в Code Review / QA Todo.`,
      thread_ts
    );
    saveStateLocal();
    scheduleSaveToGitHub();
    return;
  }

  // ── Parse manual events from message ──
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
      `Добавила ${parsed.length} событий. Всего: ${exp.suggestedEvents.length}\n\n${list}\n\nКогда готово → напишите \`approve\``,
      thread_ts
    );
    saveStateLocal();
    scheduleSaveToGitHub();
  } else {
    // Unknown message — let the user know what the bot expects
    console.log(`💬 Unrecognized reply for #${exp.sequentialId}: "${text.slice(0, 60)}"`);
  }
}

// ─── Slack polling ───
const lastSeenTs = {};

async function pollSlackThreads() {
  const active = Object.values(experiments).filter(
    (e) => e.slackThreadTs && !e.reportDone && (!e.eventsApproved || e.awaitingProperty)
  );
  if (active.length === 0) return;

  for (const exp of active) {
    try {
      const oldest = lastSeenTs[exp.cardCommonId] || exp.slackThreadTs;
      const url = `https://slack.com/api/conversations.replies?channel=${SLACK_CHANNEL_ID}&ts=${exp.slackThreadTs}&oldest=${oldest}&limit=20`;
      const res = await fetch(url, { headers: { Authorization: `Bearer ${SLACK_BOT_TOKEN}` } });
      const data = await res.json();
      if (!data.ok) { console.error("Slack poll error:", data.error); continue; }

      const messages = (data.messages || []).filter(
        (m) => m.ts !== exp.slackThreadTs && m.ts > oldest && !m.bot_id
      );
      if (messages.length > 0) {
        console.log(`📨 Poll: ${messages.length} new reply(ies) for #${exp.sequentialId}`);
      }
      for (const msg of messages) {
        await handleSlackThreadReply({ text: msg.text, thread_ts: exp.slackThreadTs, user: msg.user });
        lastSeenTs[exp.cardCommonId] = msg.ts;
      }
    } catch (e) {
      console.error(`Poll error for #${exp.sequentialId}:`, e.message);
    }
  }
}

// ─── Webhook endpoint ───
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
    timestamp: now, cardCommonId, cardId: card.cardId,
    sequentialId: card.sequentialId, name: card.name,
    columnId: currentColId, columnName: currentColName,
    assignees, timeOnColumns: card.timeOnColumns || {},
  };

  const prev = cardState[cardCommonId];
  const hasExpTag = cardHasExperimentTag(card, req);
  const isTracked = !!experiments[cardCommonId];

  if (hasExpTag && !isTracked) {
    handleExperimentTagAdded(card, now).catch(console.error);
  }

  if (prev && prev.columnId !== currentColId) {
    const fromName = prev.columnName;
    const toName = currentColName;
    const moveKey = `${fromName} -> ${toName}`;

    event.previousColumn = fromName;
    event.moveDirection = colOrder(currentColId) < colOrder(prev.columnId) ? "backward" : "forward";

    if (SIGNIFICANT_RETURNS.has(moveKey)) {
      const ret = {
        timestamp: now, cardCommonId, sequentialId: card.sequentialId,
        cardName: card.name, fromColumn: fromName, toColumn: toName,
        returnType: classifyReturn(fromName), assignees, timeOnColumns: card.timeOnColumns || {},
      };
      returns.push(ret);
      event.isReturn = true;
      event.returnType = ret.returnType;
      console.log(`⚠ RETURN: #${card.sequentialId} | ${moveKey}`);
    } else {
      console.log(`→ Move: #${card.sequentialId} "${card.name}" | ${moveKey}`);
    }

    if (hasExpTag || isTracked) {
      if (toName === "Code Review") handleCodeReview(card, now).catch(console.error);
      if (toName === "QA Todo") handleQATodo(card, now).catch(console.error);
      if (toName === "Released" || toName === "Done") handleReleased(card, now).catch(console.error);
    }
  } else if (!prev) {
    console.log(`+ New: #${card.sequentialId} "${card.name}" in ${currentColName}`);
  }

  cardState[cardCommonId] = {
    columnId: currentColId, columnName: currentColName,
    lastSeen: now, sequentialId: card.sequentialId,
    name: card.name, assignees,
  };

  allEvents.push(event);
  saveStateLocal();
  scheduleSaveToGitHub();
  res.status(200).json({ ok: true });
});

// ─── API endpoints ───
app.get("/", (req, res) => res.send("Favro webhook logger + Experiment bot"));
app.get("/health", (req, res) =>
  res.json({
    status: "ok",
    github: !!(GH_TOKEN && GH_REPO), slack: !!(SLACK_BOT_TOKEN && SLACK_CHANNEL_ID),
    favro: !!(FAVRO_EMAIL && FAVRO_API_TOKEN && FAVRO_ORG_ID),
    uptime: process.uptime(), trackedCards: Object.keys(cardState).length,
    totalEvents: allEvents.length, totalReturns: returns.length,
    activeExperiments: Object.values(experiments).filter((e) => !e.reportDone).length,
    propertyMappings: Object.keys(propertyMapping).length,
  }));
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
  res.json({ totalReturns: returns.length, byType: sort(byType), byAssignee: sort(byAssignee), byCard: sort(byCard) });
});
app.get("/api/events", (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  res.json({ total: allEvents.length, events: allEvents.slice(-limit).reverse() });
});
app.get("/api/state", (req, res) =>
  res.json({ trackedCards: Object.keys(cardState).length, cards: cardState }));
app.get("/api/experiments", (req, res) =>
  res.json({
    total: Object.keys(experiments).length,
    experiments: Object.values(experiments).sort((a, b) => new Date(b.tagDetectedAt) - new Date(a.tagDetectedAt)),
  }));
app.get("/api/property-mapping", (req, res) => res.json(propertyMapping));
app.post("/api/save", async (req, res) => {
  await saveToGitHub();
  res.json({ ok: true });
});

// ─── Start ───
async function start() {
  await loadStateFromGitHub();
  if (SLACK_BOT_TOKEN && SLACK_CHANNEL_ID) {
    setInterval(pollSlackThreads, 60 * 1000);
    console.log("Slack polling: ON (60s)");
  }
  const PORT = process.env.PORT || 10000;
  app.listen(PORT, () => {
    console.log(`Server :${PORT} | GitHub:${GH_TOKEN ? "ON" : "OFF"} Slack:${SLACK_BOT_TOKEN ? "ON" : "OFF"} Favro:${FAVRO_EMAIL ? "ON" : "OFF"}`);
  });
}

start();

const express = require("express");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(express.json({ limit: "1mb" }));

// ─── GitHub persistence config ───
const GH_TOKEN = process.env.GH_TOKEN;       // GitHub Personal Access Token
const GH_REPO = process.env.GH_REPO;         // e.g. "username/favro-webhook-logger"
const GH_BRANCH = process.env.GH_BRANCH || "main";
const GH_DATA_FILES = ["card_state.json", "returns.json", "events.json"];

// ─── Column config (All projects board) ───
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

// ─── User map ───
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

// ─── Significant returns (from → to that counts as regression) ───
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

// SHA cache for each file
const fileShas = {};

// ─── Load state from GitHub on startup ───
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

    console.log("GitHub state loaded successfully");
  } catch (e) {
    console.error("Error loading from GitHub:", e.message);
  }
}

// ─── Save state to GitHub (debounced) ───
let saveTimeout = null;
const SAVE_DELAY_MS = 5000; // batch saves: wait 5s after last event

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

// ─── Also save to local disk as fallback ───
const DATA_DIR = path.join(__dirname, "data");

function saveStateLocal() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(path.join(DATA_DIR, "card_state.json"), JSON.stringify(cardState, null, 2));
    fs.writeFileSync(path.join(DATA_DIR, "events.json"), JSON.stringify(allEvents, null, 2));
    fs.writeFileSync(path.join(DATA_DIR, "returns.json"), JSON.stringify(returns, null, 2));
  } catch (e) {
    console.error("Local save failed:", e.message);
  }
}

// ─── Helpers ───
function colName(colId) {
  return COLUMNS[colId]?.name || colId;
}
function colOrder(colId) {
  return COLUMNS[colId]?.order ?? -1;
}
function userName(userId) {
  return USERS[userId] || userId;
}
function classifyReturn(fromCol) {
  if (fromCol.includes("Code Review")) return "code_review_rejection";
  if (fromCol.includes("QA")) return "qa_rejection";
  if (fromCol.includes("Waits for Build")) return "build_rejection";
  if (fromCol.includes("Ready for Release")) return "release_rejection";
  return "other_return";
}

// ─── Webhook endpoint ───
app.post("/favro-webhook", (req, res) => {
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
        `⚠ RETURN DETECTED: #${card.sequentialId} "${card.name}" | ${moveKey} | ${returnEvent.returnType} | ${assignees.join(", ") || "unassigned"}`
      );
    } else {
      console.log(
        `→ Move: #${card.sequentialId} "${card.name}" | ${moveKey} (${event.moveDirection})`
      );
    }
  } else if (!prev) {
    console.log(
      `+ New card tracked: #${card.sequentialId} "${card.name}" in ${currentColName}`
    );
  } else {
    console.log(
      `= Same column: #${card.sequentialId} "${card.name}" still in ${currentColName}`
    );
  }

  // Update state
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

app.get("/", (req, res) => {
  res.send("Favro webhook logger is running");
});

app.get("/api/returns", (req, res) => {
  res.json({
    total: returns.length,
    returns: returns.slice().reverse(),
  });
});

app.get("/api/returns/stats", (req, res) => {
  const byType = {};
  const byAssignee = {};
  const byCard = {};

  for (const r of returns) {
    byType[r.returnType] = (byType[r.returnType] || 0) + 1;
    for (const a of r.assignees) {
      byAssignee[a] = (byAssignee[a] || 0) + 1;
    }
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
  res.json({
    total: allEvents.length,
    events: allEvents.slice(-limit).reverse(),
  });
});

app.get("/api/state", (req, res) => {
  res.json({
    trackedCards: Object.keys(cardState).length,
    cards: cardState,
  });
});

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    githubPersistence: !!(GH_TOKEN && GH_REPO),
    uptime: process.uptime(),
    trackedCards: Object.keys(cardState).length,
    totalEvents: allEvents.length,
    totalReturns: returns.length,
  });
});

// Force save (manual trigger)
app.post("/api/save", async (req, res) => {
  await saveToGitHub();
  res.json({ ok: true, message: "Saved to GitHub" });
});

// ─── Start ───
async function start() {
  await loadStateFromGitHub();
  const PORT = process.env.PORT || 10000;
  app.listen(PORT, () => {
    console.log(`Favro webhook logger listening on port ${PORT}`);
    console.log(`GitHub persistence: ${GH_TOKEN && GH_REPO ? "ENABLED" : "DISABLED"}`);
  });
}

start();

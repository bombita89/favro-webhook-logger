const express = require("express");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(express.json({ limit: "1mb" }));

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
  // Code Review → back
  "Code Review -> Doing",
  "Code Review -> Ready For Dev",
  // Waits for Build → back
  "Waits for Build -> Doing",
  "Waits for Build -> Ready For Dev",
  // QA Todo → back
  "QA Todo -> Doing",
  "QA Todo -> Ready For Dev",
  // QA Doing → back
  "QA Doing -> Doing",
  "QA Doing -> Ready For Dev",
  "QA Doing -> QA Todo",
  // Ready for Release → back
  "Ready for Release -> Doing",
  "Ready for Release -> Ready For Dev",
  "Ready for Release -> QA Todo",
  "Ready for Release -> QA Doing",
]);

// ─── In-memory state ───
// cardCommonId → { columnId, columnName, lastSeen }
const cardState = {};

// All events log (in memory, persisted to file)
const allEvents = [];
const returns = [];

// ─── File paths ───
const DATA_DIR = path.join(__dirname, "data");
const STATE_FILE = path.join(DATA_DIR, "card_state.json");
const EVENTS_FILE = path.join(DATA_DIR, "events.json");
const RETURNS_FILE = path.join(DATA_DIR, "returns.json");

// ─── Load persisted state on startup ───
function loadState() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    if (fs.existsSync(STATE_FILE)) {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
      Object.assign(cardState, data);
      console.log(`Loaded state for ${Object.keys(data).length} cards`);
    }
    if (fs.existsSync(EVENTS_FILE)) {
      const data = JSON.parse(fs.readFileSync(EVENTS_FILE, "utf8"));
      allEvents.push(...data);
      console.log(`Loaded ${data.length} historical events`);
    }
    if (fs.existsSync(RETURNS_FILE)) {
      const data = JSON.parse(fs.readFileSync(RETURNS_FILE, "utf8"));
      returns.push(...data);
      console.log(`Loaded ${data.length} historical returns`);
    }
  } catch (e) {
    console.log("No previous state found, starting fresh:", e.message);
  }
}

function saveState() {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(STATE_FILE, JSON.stringify(cardState, null, 2));
    fs.writeFileSync(EVENTS_FILE, JSON.stringify(allEvents, null, 2));
    fs.writeFileSync(RETURNS_FILE, JSON.stringify(returns, null, 2));
  } catch (e) {
    console.error("Failed to save state:", e.message);
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

function classifyReturn(fromCol, toCol) {
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

  // Log raw event
  console.log("=== FAVRO EVENT ===", now);

  // Handle test ping
  if (body.test) {
    console.log("Test ping received");
    return res.status(200).json({ ok: true, message: "pong" });
  }

  // Extract card data
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

  // Check for return (regression)
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
        returnType: classifyReturn(fromName, toName),
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
  saveState();

  res.status(200).json({ ok: true });
});

// ─── API endpoints for reading data ───

app.get("/", (req, res) => {
  res.send("Favro webhook logger is running");
});

// Get all returns
app.get("/api/returns", (req, res) => {
  res.json({
    total: returns.length,
    returns: returns.slice().reverse(), // newest first
  });
});

// Get returns summary/stats
app.get("/api/returns/stats", (req, res) => {
  const byType = {};
  const byAssignee = {};
  const byCard = {};

  for (const r of returns) {
    // By type
    byType[r.returnType] = (byType[r.returnType] || 0) + 1;

    // By assignee
    for (const a of r.assignees) {
      byAssignee[a] = (byAssignee[a] || 0) + 1;
    }

    // By card
    const key = `#${r.sequentialId} ${r.cardName}`;
    byCard[key] = (byCard[key] || 0) + 1;
  }

  // Sort
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

// Get recent events (last N)
app.get("/api/events", (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  res.json({
    total: allEvents.length,
    events: allEvents.slice(-limit).reverse(),
  });
});

// Get current state of all tracked cards
app.get("/api/state", (req, res) => {
  res.json({
    trackedCards: Object.keys(cardState).length,
    cards: cardState,
  });
});

// Health check
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    uptime: process.uptime(),
    trackedCards: Object.keys(cardState).length,
    totalEvents: allEvents.length,
    totalReturns: returns.length,
  });
});

// ─── Start ───
loadState();
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`Favro webhook logger listening on port ${PORT}`);
});

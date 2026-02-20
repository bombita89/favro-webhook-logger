// server.js — простой логгер вебхуков Favro

const express = require("express");
const fs = require("fs");
const path = require("path");

const app = express();

// Порт: в облаке Render задаёт PORT, локально используем 3001
const PORT = process.env.PORT || 3001;

// Читаем JSON-тело у запросов
app.use(express.json());

// Папка и файл, куда пишем события
const DATA_DIR = path.join(__dirname, "data");
const LOG_FILE = path.join(DATA_DIR, "favro_events.jsonl");

// Гарантируем, что папка data существует
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Healthcheck
app.get("/", (req, res) => {
  res.send("Favro webhook logger is running");
});

// Тестовый GET для "Send test" в Favro
app.get("/favro-webhook", (req, res) => {
  res.json({
    ok: true,
    method: "GET",
    message: "Favro webhook logger is alive",
  });
});

// Основной эндпоинт для вебхуков Favro (POST)
app.post("/favro-webhook", (req, res) => {
  const record = {
    ts: new Date().toISOString(),
    body: req.body,
  };

  fs.appendFile(LOG_FILE, JSON.stringify(record) + "\n", (err) => {
    if (err) {
      console.error("Ошибка записи favro_events:", err.message);
      return res.status(500).json({ ok: false });
    }

    console.log(
      "Favro event сохранён:",
      record.body && record.body.action ? record.body.action : "unknown"
    );

    res.json({ ok: true });
  });
});

// Запуск сервера
app.listen(PORT, () => {
  console.log(`Favro webhook logger listening on port ${PORT}`);
});

// server.js
// Простой логгер вебхуков Favro -> файлы в ./data

const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();

// Render даёт порт через env, локально можно 3001
const PORT = process.env.PORT || 3001;

// Чтобы читать JSON из тела запроса
app.use(express.json({ limit: '1mb' }));

// Папка для логов
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Просто проверка, что сервис жив
app.get('/', (req, res) => {
  res.send('Favro webhook logger is running');
});

// Основной вебхук
app.post('/favro-webhook', (req, res) => {
  const payload = req.body || {};

  // 1. Логируем сырой JSON в консоль (видно в Render → Logs)
  console.log('=== FAVRO RAW EVENT ===');
  console.log(JSON.stringify(payload, null, 2));
  console.log('========================');

  // 2. Сохраняем сырой JSON в файл (по одной строке JSON на событие)
  const rawRecord = {
    receivedAt: new Date().toISOString(),
    payload
  };
  const rawFile = path.join(DATA_DIR, 'favro_raw.log');
  fs.appendFileSync(rawFile, JSON.stringify(rawRecord) + '\n');

  // 3. Пытаемся вытащить понятное резюме события
  // Структура может отличаться, поэтому много "или" — потом подправим под реальный JSON.
  const card =
    payload.card ||
    payload.data?.card ||
    payload.data ||
    {};

  const summary = {
    receivedAt: new Date().toISOString(),

    eventType: payload.eventType || payload.type || payload.action || null,

    cardId: card.cardId || card.id || payload.cardId || null,
    cardName: card.name || payload.name || null,

    // Из какой колонки / статуса
    fromColumn:
      payload.fromColumn ||
      payload.oldColumn ||
      payload.previousColumn ||
      payload.fromStatus ||
      null,

    // В какую колонку / статус
    toColumn:
      payload.toColumn ||
      payload.newColumn ||
      payload.column ||
      payload.status ||
      null,

    // Можно отдельно хранить статусы, если Favro их отдаёт отдельно
    fromStatus:
      payload.fromStatus ||
      payload.previousStatus ||
      null,

    toStatus:
      payload.toStatus ||
      payload.status ||
      null
  };

  const summaryFile = path.join(DATA_DIR, 'favro_events.log');
  fs.appendFileSync(summaryFile, JSON.stringify(summary) + '\n');

  console.log('Favro event summary:', summary);

  // Favro важно получить 200 OK
  res.status(200).json({ ok: true });
});

// Старт сервера
app.listen(PORT, () => {
  console.log(`Favro webhook logger listening on port ${PORT}`);
});

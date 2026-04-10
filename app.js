const TelegramBot = require("node-telegram-bot-api");
const axios = require("axios");
const csv = require("csv-parser");
const { Parser } = require("json2csv");
const cloudinary = require("cloudinary").v2;
const streamifier = require("streamifier");
const { Readable } = require("stream");

// 🔐 HARDCODED KEYS
const TELEGRAM_TOKEN = "YOUR_TELEGRAM_TOKEN";
const API_KEY = "YOUR_API_KEY";

const CLOUD_NAME = "YOUR_CLOUD_NAME";
const CLOUD_API_KEY = "YOUR_CLOUD_API_KEY";
const CLOUD_API_SECRET = "YOUR_CLOUD_API_SECRET";

// CONFIG
const CONCURRENCY = 10;

// CLOUDINARY
cloudinary.config({
  cloud_name: CLOUD_NAME,
  api_key: CLOUD_API_KEY,
  api_secret: CLOUD_API_SECRET
});

// FIX 409 ERROR
const bot = new TelegramBot(TELEGRAM_TOKEN, {
  polling: { autoStart: false }
});

async function startBot() {
  try {
    await bot.deleteWebHook();
    bot.startPolling({ restart: true });
    console.log("Bot started");
  } catch (err) {
    console.log("Retrying bot...");
    setTimeout(startBot, 5000);
  }
}
startBot();

// CREATE LEAD API
async function createLead(row) {
  try {
    const res = await axios.post(
      "https://l.creditlinks.in:8000/api/v2/partner/create-lead",
      {
        mobileNumber: row.mobileNumber,
        firstName: row.firstName,
        lastName: row.lastName,
        pan: row.pan,
        dob: row.dob,
        email: row.email,
        pincode: row.pincode,
        monthlyIncome: parseInt(row.monthlyIncome),
        employmentStatus: parseInt(row.employmentStatus),
        employerName: row.employerName,
        officePincode: row.officePincode,
        consumerConsentDate: new Date()
          .toISOString()
          .slice(0, 19)
          .replace("T", " "),
        consumerConsentIp: "127.0.0.1",
        waitForAllOffers: 1
      },
      {
        headers: {
          apikey: API_KEY,
          "Content-Type": "application/json"
        }
      }
    );

    return {
      ...row,
      success: res.data.success,
      leadId: res.data.leadId || ""
    };

  } catch (err) {
    return {
      ...row,
      success: false,
      message: err.response?.data?.message || err.message
    };
  }
}

// BATCH PROCESS
async function processBatch(rows) {
  let results = [];

  for (let i = 0; i < rows.length; i += CONCURRENCY) {
    const batch = rows.slice(i, i + CONCURRENCY);
    const res = await Promise.all(batch.map(createLead));
    results.push(...res);

    console.log(`Processed ${Math.min(i + CONCURRENCY, rows.length)}/${rows.length}`);
  }

  return results;
}

// START COMMAND
bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, "📤 Upload CSV file");
});

// FILE HANDLER
bot.on("document", async (msg) => {
  const chatId = msg.chat.id;

  try {
    bot.sendMessage(chatId, "⚡ Processing...");

    const file = await bot.getFile(msg.document.file_id);
    const url = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${file.file_path}`;

    const response = await axios.get(url, { responseType: "arraybuffer" });

    const rows = [];
    await new Promise((resolve) => {
      Readable.from(response.data)
        .pipe(csv())
        .on("data", (d) => rows.push(d))
        .on("end", resolve);
    });

    bot.sendMessage(chatId, `📊 Total: ${rows.length}`);

    const results = await processBatch(rows);

    const buffer = Buffer.from(new Parser().parse(results));

    const upload = await new Promise((resolve, reject) => {
      const stream = cloudinary.uploader.upload_stream(
        { resource_type: "raw", folder: "telegram-leads" },
        (err, res) => (err ? reject(err) : resolve(res))
      );
      streamifier.createReadStream(buffer).pipe(stream);
    });

    await bot.sendMessage(chatId, `✅ Done\n${upload.secure_url}`);
    await bot.sendDocument(chatId, buffer, { filename: "output.csv" });

  } catch (err) {
    console.error(err);
    bot.sendMessage(chatId, "❌ Error");
  }
});

// KEEP ALIVE
setInterval(() => {
  console.log("Running...");
}, 30000);

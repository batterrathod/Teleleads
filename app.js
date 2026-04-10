const TelegramBot = require("node-telegram-bot-api");
const axios = require("axios");
const csv = require("csv-parser");
const { Parser } = require("json2csv");
const cloudinary = require("cloudinary").v2;
const streamifier = require("streamifier");
const { Readable } = require("stream");


// 🔐PUT YOUR KEYS HERE
const TELEGRAM_TOKEN = "8742242991:AAHDft6ZY7H7lMOuzFB7-zpMsr_nKYK2SHo";
const API_KEY = "4827f87b-0e70-45ac-b822-92e7b4d6a291";

const CLOUD_NAME = "dvsndenmu";
const CLOUD_API_KEY = "892768954865488";
const CLOUD_API_SECRET = "7SVc0KOWK_68zTgrQg7aCnTOlGc";
P
// ⚡ CONFIG
const CONCURRENCY = 10;

// GLOBAL ERROR HANDLING
process.on("uncaughtException", console.error);
process.on("unhandledRejection", console.error);

// CLOUDINARY CONFIG
cloudinary.config({
  cloud_name: CLOUD_NAME,
  api_key: CLOUD_API_KEY,
  api_secret: CLOUD_API_SECRET
});

// TELEGRAM BOT (FIX 409)
const bot = new TelegramBot(TELEGRAM_TOKEN, {
  polling: { autoStart: false }
});

async function startBot() {
  try {
    await bot.deleteWebHook();
    bot.startPolling({ restart: true });
    console.log("✅ Bot started");
  } catch (e) {
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
      mobileNumber: row.mobileNumber || "",
      firstName: row.firstName || "",
      lastName: row.lastName || "",
      success: res.data?.success || false,
      message: res.data?.message || "",
      leadId: res.data?.leadId || ""
    };

  } catch (err) {
    return {
      mobileNumber: row.mobileNumber || "",
      firstName: row.firstName || "",
      lastName: row.lastName || "",
      success: false,
      message: err.response?.data?.message || err.message,
      leadId: ""
    };
  }
}

// ⚡ FAST BATCH PROCESSING
async function processBatch(rows) {
  let results = [];

  for (let i = 0; i < rows.length; i += CONCURRENCY) {
    const batch = rows.slice(i, i + CONCURRENCY);

    const batchResults = await Promise.all(batch.map(createLead));
    results.push(...batchResults);

    console.log(`Processed ${Math.min(i + CONCURRENCY, rows.length)}/${rows.length}`);
  }

  return results;
}

// START COMMAND
bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, "📤 Upload your CSV file");
});

// FILE HANDLER
bot.on("document", async (msg) => {
  const chatId = msg.chat.id;

  try {
    bot.sendMessage(chatId, "⚡ Processing started...");

    // GET FILE
    const file = await bot.getFile(msg.document.file_id);
    const fileUrl = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${file.file_path}`;

    // DOWNLOAD FILE
    const response = await axios.get(fileUrl, {
      responseType: "arraybuffer"
    });

    // PARSE CSV
    const rows = [];
    await new Promise((resolve, reject) => {
      Readable.from(response.data)
        .pipe(csv())
        .on("data", (data) => rows.push(data))
        .on("end", resolve)
        .on("error", reject);
    });

    await bot.sendMessage(chatId, `📊 Total Leads: ${rows.length}`);

    // PROCESS
    const results = await processBatch(rows);

    // SAFE CSV GENERATE
    let csvData;
    try {
      const parser = new Parser({ defaultValue: "" });
      csvData = parser.parse(results);
    } catch (e) {
      console.log("CSV ERROR:", e);
      return bot.sendMessage(chatId, "❌ CSV generation failed");
    }

    const buffer = Buffer.from(csvData, "utf-8");

    // SAFE CLOUDINARY UPLOAD
    let uploadResult;
    try {
      uploadResult = await new Promise((resolve, reject) => {
        const uploadStream = cloudinary.uploader.upload_stream(
          {
            resource_type: "raw",
            folder: "telegram-leads",
            public_id: `output_${Date.now()}`
          },
          (err, res) => {
            if (err) return reject(err);
            resolve(res);
          }
        );

        streamifier.createReadStream(buffer).pipe(uploadStream);
      });
    } catch (e) {
      console.log("UPLOAD ERROR:", e);
      return bot.sendMessage(chatId, "❌ Upload failed");
    }

    // SEND RESULT
    try {
      await bot.sendMessage(chatId, `✅ Done!\n📥 ${uploadResult.secure_url}`);

      await bot.sendDocument(chatId, buffer, {
        caption: "📄 Your CSV"
      }, {
        filename: "output.csv",
        contentType: "text/csv"
      });

    } catch (e) {
      console.log("TELEGRAM ERROR:", e);
      bot.sendMessage(chatId, "❌ File send failed");
    }

  } catch (err) {
    console.error(err);
    bot.sendMessage(chatId, "❌ Error processing file");
  }
});

// KEEP ALIVE (for GitHub / Render)
setInterval(() => {
  console.log("Running...");
}, 30000);
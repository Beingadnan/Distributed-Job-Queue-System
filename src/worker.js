const { Worker } = require("bullmq");
const Redis = require("ioredis");
const { updateJobStatus, logAuditEvent } = require("./jobService");
const { deadLetterQueue } = require("./queue");
const wsManager = require("./wsManager");

// Separate Redis connection for workers (BullMQ requires maxRetriesPerRequest: null)
const workerConnection = new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: parseInt(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

/**
 * Core job processor — simulates different job types.
 * Replace these stubs with your real business logic.
 */
async function processJob(job) {
  const { name, data } = job;
  const jobId = data._jobId || job.id;

  console.log(`[Worker] Processing job "${name}" id=${jobId} attempt=${job.attemptsMade + 1}`);

  // Update DB: mark as active
  await updateJobStatus(jobId, "active", { attempts: job.attemptsMade + 1 });
  await logAuditEvent(jobId, "job.active", { attempt: job.attemptsMade + 1, queue: job.queueName });
  wsManager.broadcast({ event: "job.active", jobId, queue: job.queueName, attempt: job.attemptsMade + 1 });

  // Simulate work based on job type
  switch (name) {
    case "send-email":
      await simulateSendEmail(data);
      break;
    case "resize-image":
      await simulateResizeImage(data);
      break;
    case "generate-report":
      await simulateGenerateReport(data);
      break;
    default:
      await simulateGenericTask(data);
  }

  const result = { processedAt: new Date().toISOString(), worker: process.pid };
  await updateJobStatus(jobId, "completed", { result, attempts: job.attemptsMade + 1 });
  await logAuditEvent(jobId, "job.completed", { result });
  wsManager.broadcast({ event: "job.completed", jobId, result });

  return result;
}

// ─── Simulated job handlers ───────────────────────────────────────────────────

async function simulateSendEmail(data) {
  await sleep(Math.random() * 200 + 100);
  // 10% chance of transient failure (to demo retries)
  if (Math.random() < 0.1) throw new Error("SMTP server timeout — will retry");
}

async function simulateResizeImage(data) {
  await sleep(Math.random() * 500 + 200);
  if (Math.random() < 0.05) throw new Error("Image codec error — will retry");
}

async function simulateGenerateReport(data) {
  await sleep(Math.random() * 1000 + 500);
}

async function simulateGenericTask(data) {
  await sleep(Math.random() * 300 + 50);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── Worker event handlers ────────────────────────────────────────────────────

function attachWorkerEvents(worker, queueName) {
  worker.on("failed", async (job, err) => {
    if (!job) return;
    const jobId = job.data._jobId || job.id;
    const isExhausted = job.attemptsMade >= job.opts.attempts;

    console.error(`[Worker:${queueName}] Job ${jobId} failed (attempt ${job.attemptsMade}/${job.opts.attempts}): ${err.message}`);

    if (isExhausted) {
      // Move to dead-letter queue
      console.warn(`[Worker:${queueName}] Job ${jobId} exhausted retries — moving to DLQ`);
      await deadLetterQueue.add(job.name, {
        ...job.data,
        _originalQueue: queueName,
        _failedReason: err.message,
        _exhaustedAt: new Date().toISOString(),
      });

      await updateJobStatus(jobId, "dead", { error: err.message, attempts: job.attemptsMade });
      await logAuditEvent(jobId, "job.dead", { reason: err.message, movedToDLQ: true });
      wsManager.broadcast({ event: "job.dead", jobId, reason: err.message });
    } else {
      // Exponential backoff retry scheduled by BullMQ automatically
      const nextDelay = Math.pow(2, job.attemptsMade) * 1000;
      await updateJobStatus(jobId, "retrying", { error: err.message, attempts: job.attemptsMade });
      await logAuditEvent(jobId, "job.retry", { attempt: job.attemptsMade, nextDelayMs: nextDelay });
      wsManager.broadcast({ event: "job.retry", jobId, attempt: job.attemptsMade, nextDelayMs: nextDelay });
    }
  });

  worker.on("error", (err) => {
    console.error(`[Worker:${queueName}] Worker error:`, err.message);
  });

  console.log(`✅ Worker started for queue: ${queueName}`);
}

// ─── Spawn workers for all priority queues ────────────────────────────────────

function startWorkers() {
  const queues = ["jobs-high", "jobs-default", "jobs-low"];
  const concurrencies = { "jobs-high": 5, "jobs-default": 3, "jobs-low": 2 };

  const workers = queues.map((queueName) => {
    const worker = new Worker(queueName, processJob, {
      connection: workerConnection,
      concurrency: concurrencies[queueName] || 3,
    });
    attachWorkerEvents(worker, queueName);
    return worker;
  });

  // Graceful shutdown
  const shutdown = async () => {
    console.log("\n[Workers] Shutting down gracefully...");
    await Promise.all(workers.map((w) => w.close()));
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  return workers;
}

module.exports = { startWorkers };

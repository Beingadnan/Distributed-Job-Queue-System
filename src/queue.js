const { Queue } = require("bullmq");
const Redis = require("ioredis");

// Shared Redis connection for BullMQ queues
const connection = new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: parseInt(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null, // Required by BullMQ
});

connection.on("error", (err) => {
  console.error("Redis connection error:", err.message);
});

connection.on("connect", () => {
  console.log("✅ Redis connected");
});

// Shared default job options
const defaultJobOptions = {
  attempts: 5,
  backoff: {
    type: "exponential",
    delay: 1000, // 1s → 2s → 4s → 8s → 16s
  },
  removeOnComplete: { count: 100 },
  removeOnFail: false, // Keep failed jobs for inspection
};

// Three priority-tier queues (BullMQ doesn't allow ':' in queue names)
const highQueue    = new Queue("jobs-high",    { connection, defaultJobOptions });
const defaultQueue = new Queue("jobs-default", { connection, defaultJobOptions });
const lowQueue     = new Queue("jobs-low",     { connection, defaultJobOptions });

// Dead-letter queue — receives jobs that exhausted all retries
const deadLetterQueue = new Queue("jobs-dead", {
  connection,
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  },
});

/**
 * Add a job to the appropriate priority queue.
 * @param {string} name     - Job type/name
 * @param {object} data     - Arbitrary job payload
 * @param {string} priority - "high" | "default" | "low"
 * @param {string} jobId    - Postgres job UUID
 */
async function enqueue(name, data, priority = "default", jobId) {
  const queues = {
    high: highQueue,
    default: defaultQueue,
    low: lowQueue,
  };

  const queue = queues[priority] || defaultQueue;

  const bullJob = await queue.add(name, { ...data, _jobId: jobId }, {
    jobId: jobId, // Tie BullMQ job ID to our DB UUID
  });

  return bullJob;
}

/**
 * Get live counts for all queues.
 */
async function getQueueStats() {
  const stats = {};
  const queues = {
    high: highQueue,
    default: defaultQueue,
    low: lowQueue,
    dead: deadLetterQueue,
  };

  for (const [name, q] of Object.entries(queues)) {
    const counts = await q.getJobCounts("waiting", "active", "completed", "failed", "delayed");
    stats[name] = counts;
  }
  return stats;
}

module.exports = {
  connection,
  highQueue,
  defaultQueue,
  lowQueue,
  deadLetterQueue,
  enqueue,
  getQueueStats,
};

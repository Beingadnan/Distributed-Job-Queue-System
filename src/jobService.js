const pool = require("./db");

/**
 * Create a new job record in PostgreSQL.
 */
async function createJob({ id, name, data, priority }) {
  const result = await pool.query(
    `INSERT INTO jobs (id, name, data, priority, status, attempts, created_at, updated_at)
     VALUES ($1, $2, $3, $4, 'queued', 0, NOW(), NOW())
     RETURNING *`,
    [id, name, JSON.stringify(data), priority]
  );
  return result.rows[0];
}

/**
 * Update job status (and optionally result/error).
 */
async function updateJobStatus(jobId, status, { result = null, error = null, attempts = null } = {}) {
  const fields = ["status = $2", "updated_at = NOW()"];
  const values = [jobId, status];
  let idx = 3;

  if (result !== null) {
    fields.push(`result = $${idx++}`);
    values.push(JSON.stringify(result));
  }
  if (error !== null) {
    fields.push(`error = $${idx++}`);
    values.push(error);
  }
  if (attempts !== null) {
    fields.push(`attempts = $${idx++}`);
    values.push(attempts);
  }

  const query = `UPDATE jobs SET ${fields.join(", ")} WHERE id = $1 RETURNING *`;
  const res = await pool.query(query, values);
  return res.rows[0];
}

/**
 * Fetch a single job by ID.
 */
async function getJob(jobId) {
  const res = await pool.query("SELECT * FROM jobs WHERE id = $1", [jobId]);
  return res.rows[0] || null;
}

/**
 * List recent jobs (paginated).
 */
async function listJobs({ limit = 50, offset = 0, status = null } = {}) {
  let query = "SELECT * FROM jobs";
  const values = [];

  if (status) {
    query += " WHERE status = $1";
    values.push(status);
  }

  query += ` ORDER BY created_at DESC LIMIT $${values.length + 1} OFFSET $${values.length + 2}`;
  values.push(limit, offset);

  const res = await pool.query(query, values);
  return res.rows;
}

/**
 * Append an audit log entry for a job event.
 */
async function logAuditEvent(jobId, event, payload = {}) {
  try {
    await pool.query(
      `INSERT INTO audit_logs (job_id, event, payload, created_at)
       VALUES ($1, $2, $3, NOW())`,
      [jobId, event, JSON.stringify(payload)]
    );
  } catch (err) {
    console.error(`[AuditLog] Failed to write event '${event}' for job ${jobId}:`, err.message);
  }
}

/**
 * Get audit trail for a job.
 */
async function getAuditLogs(jobId) {
  const res = await pool.query(
    "SELECT * FROM audit_logs WHERE job_id = $1 ORDER BY created_at ASC",
    [jobId]
  );
  return res.rows;
}

module.exports = {
  createJob,
  updateJobStatus,
  getJob,
  listJobs,
  logAuditEvent,
  getAuditLogs,
};

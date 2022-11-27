export class SqlQueries {
  public static readonly LISTEN = (queueName: string): string =>
    `LISTEN ${queueName};`;
  public static readonly NOTIFY = (
    queueName: string,
    message: Record<string, unknown>,
  ): string => `NOTIFY ${queueName}, '${JSON.stringify(message)}';`;
  public static readonly CREATE_MESSAGE_QUEUE_TABLE = (): string =>
    `CREATE TABLE IF NOT EXISTS message_queue (
      id SERIAL PRIMARY KEY,
      queue_name VARCHAR(255) NOT NULL UNIQUE,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );`;
  public static readonly CREATE_MESSAGE_TABLE = (): string =>
    `CREATE TABLE IF NOT EXISTS message(
      id SERIAL PRIMARY KEY,
      msg JSONB NOT NULL,
      delay INTEGER NOT NULL DEFAULT 0,
      retry INTEGER NOT NULL DEFAULT 0,
      consumed BOOLEAN NOT NULL DEFAULT FALSE,
      queue_id INTEGER NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      FOREIGN KEY (queue_id) REFERENCES message_queue(id)
    );`;
  public static readonly CREATE_MESSAGE_QUEUE = (): string =>
    `INSERT INTO message_queue (queue_name) VALUES ($1) ON CONFLICT DO NOTHING;`;
  public static readonly CREATE_MESSAGE =
    (): string => `INSERT INTO message (msg, delay, retry, queue_id) VALUES ($1, $2, $3,
    (SELECT id FROM message_queue WHERE queue_name = $4));`;
  public static readonly GET_QUEUE_UNCONSUMED_MESSAGES = (): string =>
    `SELECT m.id, m.msg, m.delay, m.retry, m.consumed, m.created_at, q.queue_name
        FROM message m INNER JOIN message_queue q ON m.queue_id = q.id
        WHERE q.queue_name = $1 AND m.consumed = FALSE
        FOR UPDATE SKIP LOCKED;`;
  public static readonly UPDATE_MESSAGE_CONSUMED_STATUS = (
    consumed: boolean,
  ): string => `UPDATE message SET consumed = ${consumed} WHERE id = $1;`;
}

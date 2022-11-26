import { Client, Notification } from 'pg';

interface IinitPostgresMQ {
  user: string;
  host: string;
  database: string;
  password: string;
  port: number;
  type?: 'message-queue' | 'pub-sub';
}

/**
 * Start the application.
 */
const initPostgresMQ = ({
  user,
  host,
  database,
  password,
  port,
  type,
}: IinitPostgresMQ) => {
  const dbClient = new Client({
    user,
    host,
    database,
    password,
    port,
  });

  dbClient.connect((err) => {
    if (err) {
      console.log('Error connecting to database', err);
    } else {
      console.log('Connected to database');
    }
  });

  if (type === 'message-queue') {
    dbClient.query(
      `CREATE TABLE IF NOT EXISTS message_queue (
        id SERIAL PRIMARY KEY,
        queue_name VARCHAR(255) NOT NULL UNIQUE,
        message JSONB NOT NULL,
        delay INTEGER NOT NULL DEFAULT 0,
        retry INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
      )`,
    );
  }

  return dbClient;
};

class PostgresMQ {
  private dbClient: Client;
  /**
   * Start the application via the constructor.
   */
  constructor({
    user,
    database,
    host,
    password,
    port,
    type = 'pub-sub',
  }: IinitPostgresMQ) {
    this.dbClient = initPostgresMQ({
      user,
      database,
      host,
      password,
      port,
      type,
    });
  }

  /**
   * Subscribe to a channel.
   * @param queueName - The name of the queue to listen to.
   */
  public subscribeTo(queueName: string, action: (...args: any) => void): void {
    console.log('Subscribing to queue', ` "${queueName}" `);
    this.dbClient.query(`LISTEN ${queueName}`);

    this.dbClient.on('notification', (msg: Notification) => {
      action(msg);
    });
  }

  /**
   * Add a message to a queue.
   * @param param0 - the queue name and the message to send.
   */
  public publishMessage({
    queueName,
    message,
  }: {
    queueName: string;
    message: Record<string, unknown>;
  }): void {
    console.log('Adding message to queue', ` "${queueName}" `);
    this.dbClient.query(`NOTIFY ${queueName}, '${JSON.stringify(message)}'`);
  }

  /**
   * Add a message to a queue.
   * @param param0 - the queue name and the message to send.
   * @param param1 - the number of seconds to wait before consuming the message.
   * @param param2 - the number of times to retry consuming the message.
   * @param param3 - the number of seconds to wait before retrying to consume the message.
   */
  public async produceMessage({
    queueName,
    message,
    delay = 0,
    retry = 0,
  }: {
    queueName: string;
    message: Record<string, unknown>;
    delay?: number;
    retry?: number;
  }): Promise<void> {
    console.log('Adding message to queue', ` "${queueName}" `);
    await this.dbClient.query(
      `INSERT INTO message_queue (queue_name, message, delay, retry) VALUES ($1, $2, $3, $4)
      ON CONFLICT (queue_name) DO UPDATE SET message = message_queue.message || $2::jsonb WHERE message_queue.queue_name = $1`,
      [queueName, JSON.stringify(message), delay, retry],
    );
    this.publishMessage({ queueName, message });
  }

  /**
   * Consume a message from a queue.
   * @param queueName - the name of the queue to consume from.
   */
  public async consumeMessage(
    queueName: string,
    action: (...args: any) => void,
  ): Promise<void> {
    this.subscribeTo(queueName, async (msg: Notification) => {
      console.log('Message received', {
        channel: msg.channel,
        payload: msg.payload ? JSON.parse(msg.payload) : '<empty-value>',
      });
      const rows = await this.dbClient.query(
        `SELECT * FROM message_queue WHERE queue_name = $1`,
        [queueName],
      );
      if (rows.rowCount > 0) {
        rows.rows.forEach(async (row) => {
          const { id, message, delay, created_at } = row;
          if (delay > 0) {
            setTimeout(() => {
              action({
                id,
                created_at,
                ...message,
              });
            }, delay * 1000);
          }
          action({
            id,
            created_at,
            ...message,
          });

          await this.dbClient.query(`DELETE FROM message_queue WHERE id = $1`, [
            id,
          ]);
        });
      }
    });
  }
}

export default PostgresMQ;

import { Client, Notification } from 'pg';
import { SqlQueries } from './sql-queries';

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
      `
      ${SqlQueries.CREATE_MESSAGE_QUEUE_TABLE()}
      ${SqlQueries.CREATE_MESSAGE_TABLE()}
      `,
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
    this.dbClient.query(SqlQueries.LISTEN(queueName));

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
    this.dbClient.query(SqlQueries.NOTIFY(queueName, message));
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
    await this.dbClient.query(SqlQueries.CREATE_MESSAGE_QUEUE(), [queueName]);
    await this.dbClient.query(SqlQueries.CREATE_MESSAGE(), [
      message,
      delay,
      retry,
      queueName,
    ]);
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
        SqlQueries.GET_QUEUE_UNCONSUMED_MESSAGES(),
        [queueName],
      );

      if (rows.rowCount > 0) {
        rows.rows.forEach(async (row) => {
          const { id, msg, delay, created_at } = row;
          if (delay > 0) {
            setTimeout(() => {
              action({
                id,
                created_at,
                ...msg,
              });
            }, delay * 1000);

            await this.dbClient.query(
              SqlQueries.UPDATE_MESSAGE_CONSUMED_STATUS(true),
              [id],
            );

            return;
          }
          action({
            id,
            created_at,
            ...msg,
          });

          /* essa linha deve mesmo ser atualizada? os consumers que perderem a
          mensagem não vão conseguir consumir novamente? */
          await this.dbClient.query(
            SqlQueries.UPDATE_MESSAGE_CONSUMED_STATUS(true),
            [id],
          );
        });
      }
    });
  }
}

export default PostgresMQ;

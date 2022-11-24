import { Client, Notification } from 'pg';

interface IinitPostgresMQ {
  user: string;
  host: string;
  database: string;
  password: string;
  port: number;
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

  return dbClient;
};

class PostgresMQ {
  private dbClient: Client;
  /**
   * Start the application via the constructor.
   */
  constructor({ user, database, host, password, port }: IinitPostgresMQ) {
    this.dbClient = initPostgresMQ({
      user,
      database,
      host,
      password,
      port,
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
}

export default PostgresMQ;

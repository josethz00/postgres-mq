import { Notification } from 'pg';
import PostgresMQ from './main';

const myQueue = new PostgresMQ({
  user: 'postgres-mq',
  database: 'postgres-mq',
  host: 'localhost',
  password: 'postgres-mq',
  port: 5439,
});

/**
 *
 * @param _
 */
function sayHi(_: string) {
  console.log(`Hi, ${_} !`);
}

myQueue.subscribeTo('my_test_queue', (msg: Notification) => {
  console.log('Message received', {
    channel: msg.channel,
    payload: msg.payload ? JSON.parse(msg.payload) : '<empty-value>',
  });
  sayHi(msg.channel);
});

setInterval(() => {
  myQueue.publishMessage({
    queueName: 'my_test_queue',
    message: {
      foo: 'bar',
      it: 'works',
    },
  });
}, 5000);

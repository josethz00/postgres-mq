import PostgresMQ from './main';

const myQueue = new PostgresMQ({
  user: 'postgres-mq',
  database: 'postgres-mq',
  host: 'localhost',
  password: 'postgres-mq',
  port: 5439,
  type: 'message-queue',
});

/**
 *
 * @param _
 */
function sayHi(_: unknown) {
  console.log(`Hi, ${JSON.stringify(_)} !`);
}

setInterval(() => {
  myQueue.produceMessage({
    queueName: 'my_test_queue',
    message: {
      foo: 'bar',
      it: 'works',
    },
  });
}, 5000);

myQueue.consumeMessage('my_test_queue', sayHi);

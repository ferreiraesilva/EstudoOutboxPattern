import { Kafka } from 'kafkajs';

const kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
const topic = process.env.TOPIC || 'app.public.outbox_events';
const groupId = process.env.GROUP_ID || 'email-consumer-group-v1';

const kafka = new Kafka({ clientId: 'email-service', brokers: [kafkaBroker] });
const consumer = kafka.consumer({ groupId });
const processedPessoas = new Set();

const run = async () => {
  let connected = false;
  while (!connected) {
    try {
      await consumer.connect();
      connected = true;
      console.log('[Email Service] Connected to Kafka.');
    } catch (err) {
      await new Promise(res => setTimeout(res, 3000));
    }
  }

  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const rawValue = message.value ? message.value.toString() : null;
        if (!rawValue) return;

        const record = JSON.parse(rawValue);
        if (!record.after || !record.after.payload) return;

        let payload = record.after.payload;
        if (typeof payload === 'string') payload = JSON.parse(payload);

        const pessoaId = payload.pessoa_id;
        if (processedPessoas.has(pessoaId)) {
          console.log(`[Email Service] ⚠️ Message for pessoa_id ${pessoaId} already processed (Idempotent bypass).`);
          return;
        }

        processedPessoas.add(pessoaId);

        console.log('\n==================================================');
        console.log(`✉️ [Email Service] Sending Welcome Email!`);
        console.log(`Target Email: ${payload.email}`);
        console.log(`Subject: Seja bem-vindo(a), ${payload.nome}!`);
        console.log(`Address Info: ${payload.endereco}`);
        console.log(`Status: SENT (Idempotency Key: ${pessoaId})`);
        console.log('==================================================\n');
      } catch (err) {
        console.error('[Email Service] Error:', err.message);
      }
    }
  });
};

run();

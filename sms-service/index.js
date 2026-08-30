import { Kafka } from 'kafkajs';

const kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
const topic = process.env.TOPIC || 'app.public.outbox_events';
const groupId = process.env.GROUP_ID || 'sms-consumer-group-v1';

const kafka = new Kafka({ clientId: 'sms-service', brokers: [kafkaBroker] });
const consumer = kafka.consumer({ groupId });
const processedPessoas = new Set();

const run = async () => {
  let connected = false;
  while (!connected) {
    try {
      await consumer.connect();
      connected = true;
      console.log('[SMS Service] Connected to Kafka.');
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
          console.log(`[SMS Service] ⚠️ Message for pessoa_id ${pessoaId} already processed (Idempotent bypass).`);
          return;
        }

        processedPessoas.add(pessoaId);

        console.log('\n==================================================');
        console.log(`💬 [SMS Service] Sending Security Activation SMS!`);
        console.log(`Target Number: (${payload.ddd}) ${payload.numero}`);
        console.log(`SMS Content: "Prezado(a) ${payload.nome}, seu código de validação é: 849-201"`);
        console.log(`Status: SENT (Idempotency Key: ${pessoaId})`);
        console.log('==================================================\n');
      } catch (err) {
        console.error('[SMS Service] Error:', err.message);
      }
    }
  });
};

run();

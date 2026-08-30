import { Kafka } from 'kafkajs';

const kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
const groupId = process.env.GROUP_ID || 'cdc-strategy-b-group-v2';

const topics = [
  'app.public.pessoas',
  'app.public.emails',
  'app.public.enderecos',
  'app.transaction'
];

const kafka = new Kafka({
  clientId: 'cdc-strategy-b-consumer',
  brokers: [kafkaBroker]
});

const consumer = kafka.consumer({ groupId });

// In-memory buffer storing transaction state: txId -> { expectedCount, receivedCount, pessoa, emails, enderecos }
const pendingTransactions = new Map();

function getOrCreateTxBuffer(txId) {
  const normalizedTxId = String(txId).split(':')[0];
  if (!pendingTransactions.has(normalizedTxId)) {
    pendingTransactions.set(normalizedTxId, {
      txId: normalizedTxId,
      expectedCount: null,
      receivedCount: 0,
      pessoa: null,
      emails: [],
      enderecos: []
    });
  }
  return pendingTransactions.get(normalizedTxId);
}

function checkAndFlush(txId) {
  const normalizedTxId = String(txId).split(':')[0];
  const tx = pendingTransactions.get(normalizedTxId);
  if (!tx) return;

  if (tx.expectedCount !== null && tx.receivedCount >= tx.expectedCount) {
    console.log('\n==================================================');
    console.log(`[Consumer Service] 🎉 CONSOLIDATED EVENT (Strategy B)`);
    console.log(`Transaction ID: ${normalizedTxId} | Total Events Captured: ${tx.receivedCount}/${tx.expectedCount}`);
    
    console.log('\n--- Aggregated Person Payload ---');
    console.log(JSON.stringify({
      transaction_id: normalizedTxId,
      pessoa: tx.pessoa,
      emails: tx.emails,
      enderecos: tx.enderecos
    }, null, 2));
    console.log('==================================================\n');

    pendingTransactions.delete(normalizedTxId);
  }
}

const run = async () => {
  let connected = false;
  while (!connected) {
    try {
      console.log(`[Consumer Service] Connecting to Kafka at ${kafkaBroker}...`);
      await consumer.connect();
      connected = true;
      console.log('[Consumer Service] Successfully connected to Kafka.');
    } catch (err) {
      console.error('[Consumer Service] Connection failed, retrying in 3s...', err.message);
      await new Promise(res => setTimeout(res, 3000));
    }
  }

  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: true });
    console.log(`[Consumer Service] Subscribed to topic: ${topic}`);
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const rawValue = message.value ? message.value.toString() : null;
        if (!rawValue) return;

        const record = JSON.parse(rawValue);

        // 1. Handle Transaction Control Topic (app.transaction)
        if (topic === 'app.transaction') {
          const { status, id, event_count } = record;
          const normalizedTxId = String(id).split(':')[0];
          console.log(`[CDC Transaction Marker] Status: ${status} | Raw TxID: ${id} (Parsed: ${normalizedTxId}) | Expected Event Count: ${event_count || 'N/A'}`);

          if (status === 'END' && id) {
            const txBuffer = getOrCreateTxBuffer(normalizedTxId);
            txBuffer.expectedCount = event_count;
            checkAndFlush(normalizedTxId);
          }
          return;
        }

        // 2. Handle Data Change Events (pessoas, emails, enderecos)
        const rawTxId = record.source?.txId || record.transaction?.id;
        const tableName = record.source?.table;

        if (!rawTxId || !tableName || !record.after) return;

        const normalizedTxId = String(rawTxId).split(':')[0];
        console.log(`[CDC Event Captured] Table: ${tableName} | TxID: ${normalizedTxId} | Op: ${record.op}`);

        const txBuffer = getOrCreateTxBuffer(normalizedTxId);
        txBuffer.receivedCount += 1;

        if (tableName === 'pessoas') {
          txBuffer.pessoa = record.after;
        } else if (tableName === 'emails') {
          txBuffer.emails.push(record.after);
        } else if (tableName === 'enderecos') {
          txBuffer.enderecos.push(record.after);
        }

        checkAndFlush(normalizedTxId);
      } catch (err) {
        console.error('[Consumer Service] Error processing message:', err.message);
      }
    }
  });
};

run().catch(err => {
  console.error('[Consumer Service] Fatal error:', err);
  process.exit(1);
});

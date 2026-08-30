import { Kafka } from 'kafkajs';
import pkg from 'pg';

const { Pool } = pkg;

const dbPool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://appuser:apppass@postgres:5432/appdb'
});

const kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
const groupId = process.env.GROUP_ID || 'aggregator-worker-group-v1';

const kafka = new Kafka({
  clientId: 'aggregator-worker',
  brokers: [kafkaBroker]
});

const consumer = kafka.consumer({ groupId });
const pendingTransactions = new Map();

function getOrCreateTxBuffer(txId) {
  const normalizedTxId = String(txId).split(':')[0];
  if (!pendingTransactions.has(normalizedTxId)) {
    pendingTransactions.set(normalizedTxId, {
      txId: normalizedTxId,
      expectedCount: null,
      receivedCount: 0,
      pessoa: null,
      email: null,
      endereco: null,
      telefone: null
    });
  }
  return pendingTransactions.get(normalizedTxId);
}

async function checkAndProcessTx(txId) {
  const normalizedTxId = String(txId).split(':')[0];
  const tx = pendingTransactions.get(normalizedTxId);
  if (!tx) return;

  if (tx.expectedCount !== null && tx.receivedCount >= tx.expectedCount) {
    if (tx.pessoa) {
      console.log(`\n[Aggregator Worker] ⚙️ Consolidating 4 ERP tables for TxID: ${normalizedTxId}`);

      const consolidatedPayload = {
        pessoa_id: tx.pessoa.id,
        nome: tx.pessoa.nome,
        cpf: tx.pessoa.cpf,
        email: tx.email ? tx.email.email : null,
        endereco: tx.endereco ? `${tx.endereco.logradouro} - ${tx.endereco.cidade}/${tx.endereco.uf}` : null,
        ddd: tx.telefone ? tx.telefone.ddd : null,
        numero: tx.telefone ? tx.telefone.numero : null,
        telefone_formatado: tx.telefone ? `(${tx.telefone.ddd}) ${tx.telefone.numero}` : null,
        created_at: tx.pessoa.created_at
      };

      try {
        const client = await dbPool.connect();
        try {
          const insertQuery = `
            INSERT INTO outbox_events (event_type, aggregate_id, payload, status)
            VALUES ($1, $2, $3, $4)
            RETURNING id;
          `;
          const res = await client.query(insertQuery, [
            'USUARIO_CADASTRADO',
            tx.pessoa.id,
            JSON.stringify(consolidatedPayload),
            'PENDING'
          ]);
          console.log(`[Aggregator Worker] ✅ Successfully inserted consolidated event into outbox_events (ID: ${res.rows[0].id})\n`);
        } finally {
          client.release();
        }
      } catch (err) {
        console.error('[Aggregator Worker] Error writing to outbox_events:', err.message);
      }
    }
    pendingTransactions.delete(normalizedTxId);
  }
}

const run = async () => {
  let connected = false;
  while (!connected) {
    try {
      console.log(`[Aggregator Worker] Connecting to Kafka at ${kafkaBroker}...`);
      await consumer.connect();
      connected = true;
      console.log('[Aggregator Worker] Successfully connected to Kafka.');
    } catch (err) {
      console.error('[Aggregator Worker] Connection failed, retrying in 3s...', err.message);
      await new Promise(res => setTimeout(res, 3000));
    }
  }

  const topics = [
    'app.public.pessoas',
    'app.public.emails',
    'app.public.enderecos',
    'app.public.telefones',
    'app.transaction'
  ];

  for (const topic of topics) {
    await consumer.subscribe({ topic, fromBeginning: true });
    console.log(`[Aggregator Worker] Subscribed to topic: ${topic}`);
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const rawValue = message.value ? message.value.toString() : null;
        if (!rawValue) return;

        const record = JSON.parse(rawValue);

        // 1. Transaction Control Topic
        if (topic === 'app.transaction') {
          const { status, id, event_count } = record;
          const normalizedTxId = String(id).split(':')[0];

          if (status === 'END' && id) {
            const txBuffer = getOrCreateTxBuffer(normalizedTxId);
            txBuffer.expectedCount = event_count;
            await checkAndProcessTx(normalizedTxId);
          }
          return;
        }

        // 2. Data Change Events
        const rawTxId = record.source?.txId || record.transaction?.id;
        const tableName = record.source?.table;

        if (!rawTxId || !tableName || !record.after) return;

        const normalizedTxId = String(rawTxId).split(':')[0];
        const txBuffer = getOrCreateTxBuffer(normalizedTxId);
        txBuffer.receivedCount += 1;

        if (tableName === 'pessoas') {
          txBuffer.pessoa = record.after;
        } else if (tableName === 'emails') {
          txBuffer.email = record.after;
        } else if (tableName === 'enderecos') {
          txBuffer.endereco = record.after;
        } else if (tableName === 'telefones') {
          txBuffer.telefone = record.after;
        }

        await checkAndProcessTx(normalizedTxId);
      } catch (err) {
        console.error('[Aggregator Worker] Error processing message:', err.message);
      }
    }
  });
};

run().catch(err => {
  console.error('[Aggregator Worker] Fatal error:', err);
  process.exit(1);
});

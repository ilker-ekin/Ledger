import { KafkaJS } from '@confluentinc/kafka-javascript';

const TOPIC = 'ledger-events';
const GROUPS = ['live', 'batch'];
const WATCH_MS = 2_000;

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ['localhost:9092'] } });
const admin = kafka.admin();

// Lag isn't stored in Kafka — it's derived from two numbers:
//   lag = log-end offset (from the partition) − committed offset (from the group)
async function report(): Promise<string[]> {
  // Per partition: `high` = next offset to be written, `low` = oldest still retained.
  const ends = (await admin.fetchTopicOffsets(TOPIC)).sort((a, b) => a.partition - b.partition);
  const lines = [['group', ...ends.map((e) => `p${e.partition}`), 'total'].map((c) => c.padStart(6)).join('')];

  for (const groupId of GROUPS) {
    const [committed] = await admin.fetchOffsets({ groupId, topics: [TOPIC] });
    const lags = ends.map(({ partition, high, low }) => {
      const offset = committed?.partitions.find((p) => p.partition === partition)?.offset ?? '-1';
      // -1 = the group never committed here, so everything still retained is unread.
      return Number(high) - Number(offset === '-1' ? low : offset);
    });
    const total = lags.reduce((sum, lag) => sum + lag, 0);
    lines.push([groupId, ...lags, total].map((c) => String(c).padStart(6)).join(''));
  }

  return lines;
}

await admin.connect();

if (process.argv.includes('--watch')) {
  // Redraw every 2s until Ctrl+C.
  process.on('SIGINT', async () => {
    await admin.disconnect();
    process.exit(0);
  });
  while (true) {
    const lines = await report();
    console.clear();
    console.log(`consumer lag on ${TOPIC} — ${new Date().toLocaleTimeString()} (Ctrl+C to stop)\n`);
    console.log(lines.join('\n'));
    await new Promise((resolve) => setTimeout(resolve, WATCH_MS));
  }
} else {
  console.log((await report()).join('\n'));
  await admin.disconnect();
}

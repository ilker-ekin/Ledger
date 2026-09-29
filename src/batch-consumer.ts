import { mkdirSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { KafkaJS } from '@confluentinc/kafka-javascript';

const TOPIC = 'ledger-events';
const GROUP_ID = 'batch';
const OFFSET_BEGINNING = -2; // librdkafka's "start of the partition"

// Stop once no new event has arrived for this long — "caught up".
const IDLE_MS = 3_000;
// Joining the group takes a few seconds, so allow longer for the first event.
const FIRST_EVENT_GRACE_MS = 10_000;

type Tier = 'T1' | 'T2' | 'T3';

// Same event shape as the producer (duplicated on purpose — no shared module).
type EventBody =
  | { type: 'order.created'; data: { orderId: string; amountCents: number } }
  | { type: 'payment.processed'; data: { orderId: string; amountCents: number } }
  | { type: 'user.tier_changed'; data: { fromTier: Tier; toTier: Tier } };

type LedgerEvent = EventBody & {
  eventId: string;
  userId: string;
  occurredAt: string;
};

// Rollup per hour of business time (occurredAt), not Kafka's write time.
type HourRollup = { orders: number; revenueCents: number; tierChanges: number };

// Stored hourly totals and the position they cover live in one SQLite file
// and are saved in one transaction per run — a crash can never leave totals
// saved without their position, so events are never counted twice.
mkdirSync('data', { recursive: true });
const db = new DatabaseSync('data/batch.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS rollup (
    hour          TEXT PRIMARY KEY,
    orders        INTEGER NOT NULL,
    revenue_cents INTEGER NOT NULL,
    tier_changes  INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS offsets (
    partition   INTEGER PRIMARY KEY,
    next_offset INTEGER NOT NULL
  );
`);

const addToRollup = db.prepare(`
  INSERT INTO rollup VALUES (?, ?, ?, ?)
  ON CONFLICT(hour) DO UPDATE SET
    orders        = orders + excluded.orders,
    revenue_cents = revenue_cents + excluded.revenue_cents,
    tier_changes  = tier_changes + excluded.tier_changes
`);
const getOffset = db.prepare('SELECT next_offset AS nextOffset FROM offsets WHERE partition = ?');
const saveOffset = db.prepare('INSERT OR REPLACE INTO offsets VALUES (?, ?)');
const allTotals = db.prepare(
  'SELECT hour, orders, revenue_cents AS revenueCents, tier_changes AS tierChanges FROM rollup ORDER BY hour',
);

function savedOffset(partition: number): number | undefined {
  return (getOffset.get(partition) as { nextOffset: number } | undefined)?.nextOffset;
}

// This run's changes, held in memory until the single transaction at the end.
const runRollup = new Map<string, HourRollup>();
// First and last offset read per partition — shows exactly what this run covered.
const ranges = new Map<number, { first: string; last: string }>();
let eventCount = 0;

function apply(event: LedgerEvent) {
  const hour = event.occurredAt.slice(0, 13) + ':00Z'; // e.g. 2026-09-28T10:00Z
  const row = runRollup.get(hour) ?? { orders: 0, revenueCents: 0, tierChanges: 0 };

  switch (event.type) {
    case 'order.created':
      row.orders += 1;
      break;
    case 'payment.processed':
      row.revenueCents += event.data.amountCents;
      break;
    case 'user.tier_changed':
      row.tierChanges += 1;
      break;
  }

  runRollup.set(hour, row);
}

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ['localhost:9092'] } });

const consumer = kafka.consumer({
  // Kafka offsets are still committed after each run, but only so the lag
  // monitor can see batch's progress — the real position is in SQLite.
  kafkaJS: { groupId: GROUP_ID, fromBeginning: true, autoCommit: false },

  // When partitions are assigned, start each one from the offset saved in
  // SQLite (or from the beginning if this database has never seen it),
  // ignoring Kafka's committed offset.
  rebalance_cb: (err: { code: number }, assignment: { topic: string; partition: number }[]) => {
    if (err.code !== KafkaJS.ErrorCodes.ERR__ASSIGN_PARTITIONS) return;
    return assignment.map((tp) => ({ ...tp, offset: savedOffset(tp.partition) ?? OFFSET_BEGINNING }));
  },
});

await consumer.connect();
await consumer.subscribe({ topics: [TOPIC] });

let lastEventAt = Date.now() + FIRST_EVENT_GRACE_MS - IDLE_MS;

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    // Already covered by the stored totals — skip, so nothing is counted twice.
    if (Number(message.offset) < (savedOffset(partition) ?? 0)) return;

    const event = JSON.parse(message.value!.toString()) as LedgerEvent;
    apply(event);

    eventCount += 1;
    const range = ranges.get(partition);
    ranges.set(partition, { first: range?.first ?? message.offset, last: message.offset });
    lastEventAt = Date.now();
  },
});

const idleCheck = setInterval(async () => {
  if (Date.now() - lastEventAt < IDLE_MS) return;
  clearInterval(idleCheck);

  // Stop fetching first, so no event can slip in between saving and committing.
  consumer.pause([{ topic: TOPIC }]);

  // One transaction: add this run's totals and move the saved position.
  db.exec('BEGIN');
  for (const [hour, row] of runRollup) {
    addToRollup.run(hour, row.orders, row.revenueCents, row.tierChanges);
  }
  for (const [partition, { last }] of ranges) {
    saveOffset.run(partition, Number(last) + 1);
  }

  // Demo only: crash with the transaction still open. SQLite rolls it back,
  // so neither the totals nor the position are saved — the next run redoes
  // these events exactly once.
  if (process.env.CRASH_BEFORE_COMMIT) {
    console.log(`[batch] read ${eventCount} events — crashing before the transaction commits`);
    process.exit(1);
  }

  db.exec('COMMIT');

  console.log(`[batch] this run applied ${eventCount} events`);
  for (const [partition, { first, last }] of [...ranges].sort(([a], [b]) => a - b)) {
    console.log(`  partition ${partition}: offsets ${first}–${last}`);
  }
  console.log('[batch] stored hourly totals:');
  for (const row of allTotals.all() as (HourRollup & { hour: string })[]) {
    console.log(
      `  ${row.hour}  orders: ${row.orders}, revenue: ${row.revenueCents}¢, tier changes: ${row.tierChanges}`,
    );
  }

  await consumer.commitOffsets();
  await consumer.disconnect();
  process.exit(0);
}, 500);

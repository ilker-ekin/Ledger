import { mkdirSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { KafkaJS } from '@confluentinc/kafka-javascript';

const TOPIC = 'ledger-events';
const GROUP_ID = 'live';
const OFFSET_BEGINNING = -2; // librdkafka's "start of the partition"

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

// The read model: current state per user, derived from events.
type UserView = { orders: number; paidCents: number; tier: Tier | '?' };

// State and position live in the same SQLite file and are written in the same
// transaction, so they can never disagree — survives restarts and rebalances.
mkdirSync('data', { recursive: true });
const db = new DatabaseSync('data/live.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS views (
    user_id    TEXT PRIMARY KEY,
    orders     INTEGER NOT NULL,
    paid_cents INTEGER NOT NULL,
    tier       TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS offsets (
    partition   INTEGER PRIMARY KEY,
    next_offset INTEGER NOT NULL
  );
`);

const getView = db.prepare('SELECT orders, paid_cents AS paidCents, tier FROM views WHERE user_id = ?');
const saveView = db.prepare('INSERT OR REPLACE INTO views VALUES (?, ?, ?, ?)');
const getOffset = db.prepare('SELECT next_offset AS nextOffset FROM offsets WHERE partition = ?');
const saveOffset = db.prepare('INSERT OR REPLACE INTO offsets VALUES (?, ?)');

function savedOffset(partition: number): number | undefined {
  return (getOffset.get(partition) as { nextOffset: number } | undefined)?.nextOffset;
}

function apply(event: LedgerEvent): UserView {
  const view = (getView.get(event.userId) as UserView | undefined) ?? { orders: 0, paidCents: 0, tier: '?' };

  switch (event.type) {
    case 'order.created':
      view.orders += 1;
      break;
    case 'payment.processed':
      view.paidCents += event.data.amountCents;
      break;
    case 'user.tier_changed':
      view.tier = event.data.toTier;
      break;
  }

  saveView.run(event.userId, view.orders, view.paidCents, view.tier);
  return view;
}

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ['localhost:9092'] } });

const consumer = kafka.consumer({
  // autoCommit stays on only so the lag monitor can see live's progress —
  // the real position is the one saved in SQLite.
  kafkaJS: { groupId: GROUP_ID, fromBeginning: true },

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

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    // Already applied (e.g. redelivered after a crash) — skip, so each event
    // changes the view exactly once even though delivery is at-least-once.
    if (Number(message.offset) < (savedOffset(partition) ?? 0)) return;

    const event = JSON.parse(message.value!.toString()) as LedgerEvent;

    db.exec('BEGIN');
    const view = apply(event);
    saveOffset.run(partition, Number(message.offset) + 1);
    db.exec('COMMIT');

    console.log(
      `[live] p${partition}@${message.offset}  ${event.userId}  ${event.type.padEnd(18)} → orders: ${view.orders}, paid: ${view.paidCents}¢, tier: ${view.tier}`,
    );
  },
});

// Ctrl+C: leave the group cleanly, so final offsets are committed.
process.on('SIGINT', async () => {
  await consumer.disconnect();
  process.exit(0);
});

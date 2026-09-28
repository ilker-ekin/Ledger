import { KafkaJS } from '@confluentinc/kafka-javascript';

const TOPIC = 'ledger-events';
const GROUP_ID = 'batch';

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

const rollup = new Map<string, HourRollup>();
// First and last offset read per partition — shows exactly what this run covered.
const ranges = new Map<number, { first: string; last: string }>();
let eventCount = 0;

function apply(event: LedgerEvent) {
  const hour = event.occurredAt.slice(0, 13) + ':00Z'; // e.g. 2026-09-28T10:00Z
  const row = rollup.get(hour) ?? { orders: 0, revenueCents: 0, tierChanges: 0 };

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

  rollup.set(hour, row);
}

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ['localhost:9092'] } });

// autoCommit off: offsets are committed once, only after the rollup is printed.
const consumer = kafka.consumer({
  kafkaJS: { groupId: GROUP_ID, fromBeginning: true, autoCommit: false },
});

await consumer.connect();
await consumer.subscribe({ topics: [TOPIC] });

let lastEventAt = Date.now() + FIRST_EVENT_GRACE_MS - IDLE_MS;

await consumer.run({
  eachMessage: async ({ partition, message }) => {
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

  // Stop fetching first, so no event can slip in between printing and committing.
  consumer.pause([{ topic: TOPIC }]);

  console.log(`[batch] processed ${eventCount} events`);
  for (const [partition, { first, last }] of [...ranges].sort(([a], [b]) => a - b)) {
    console.log(`  partition ${partition}: offsets ${first}–${last}`);
  }
  for (const [hour, row] of [...rollup].sort()) {
    console.log(
      `  ${hour}  orders: ${row.orders}, revenue: ${row.revenueCents}¢, tier changes: ${row.tierChanges}`,
    );
  }

  // Demo only: crash after the work but before the commit, to prove
  // at-least-once — the next run must reprocess these same events.
  if (process.env.CRASH_BEFORE_COMMIT) {
    console.log('[batch] crashing before commit');
    process.exit(1);
  }

  await consumer.commitOffsets();
  console.log('[batch] offsets committed');
  await consumer.disconnect();
  process.exit(0);
}, 500);

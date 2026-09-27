import { KafkaJS } from '@confluentinc/kafka-javascript';

const TOPIC = 'ledger-events';
const GROUP_ID = 'live';

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
// In memory only — lost on restart (the problem v3 replay solves).
type UserView = { orders: number; paidCents: number; tier: Tier | '?' };

const views = new Map<string, UserView>();

function apply(event: LedgerEvent): UserView {
  const view = views.get(event.userId) ?? { orders: 0, paidCents: 0, tier: '?' };

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

  views.set(event.userId, view);
  return view;
}

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ['localhost:9092'] } });

// fromBeginning: a brand-new group starts at the beginning of the log.
// autoCommit is left at its default (on, periodic).
const consumer = kafka.consumer({ kafkaJS: { groupId: GROUP_ID, fromBeginning: true } });

await consumer.connect();
await consumer.subscribe({ topics: [TOPIC] });

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const event = JSON.parse(message.value!.toString()) as LedgerEvent;
    const view = apply(event);

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

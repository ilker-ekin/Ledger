import { randomUUID } from 'node:crypto';
import { KafkaJS } from '@confluentinc/kafka-javascript';

const TOPIC = 'ledger-events';

type Tier = 'T1' | 'T2' | 'T3';

// The type-specific part of an event. `type` decides the shape of `data`.
type EventBody =
  | { type: 'order.created'; data: { orderId: string; amountCents: number } }
  | { type: 'payment.processed'; data: { orderId: string; amountCents: number } }
  | { type: 'user.tier_changed'; data: { fromTier: Tier; toTier: Tier } };

// The full envelope published to Kafka.
type LedgerEvent = EventBody & {
  eventId: string;
  userId: string;
  occurredAt: string;
};

// A fixed scenario: 6 users each order and pay, then two change tier.
// Users are interleaved so each user's events are spread out in time,
// which is what makes per-user ordering worth checking.
const scenario: [userId: string, body: EventBody][] = [
  ['user-1', { type: 'order.created', data: { orderId: 'order-1', amountCents: 4999 } }],
  ['user-2', { type: 'order.created', data: { orderId: 'order-2', amountCents: 1500 } }],
  ['user-3', { type: 'order.created', data: { orderId: 'order-3', amountCents: 12000 } }],
  ['user-4', { type: 'order.created', data: { orderId: 'order-4', amountCents: 899 } }],
  ['user-5', { type: 'order.created', data: { orderId: 'order-5', amountCents: 25000 } }],
  ['user-6', { type: 'order.created', data: { orderId: 'order-6', amountCents: 3200 } }],
  ['user-1', { type: 'payment.processed', data: { orderId: 'order-1', amountCents: 4999 } }],
  ['user-2', { type: 'payment.processed', data: { orderId: 'order-2', amountCents: 1500 } }],
  ['user-3', { type: 'payment.processed', data: { orderId: 'order-3', amountCents: 12000 } }],
  ['user-4', { type: 'payment.processed', data: { orderId: 'order-4', amountCents: 899 } }],
  ['user-5', { type: 'payment.processed', data: { orderId: 'order-5', amountCents: 25000 } }],
  ['user-6', { type: 'payment.processed', data: { orderId: 'order-6', amountCents: 3200 } }],
  ['user-3', { type: 'user.tier_changed', data: { fromTier: 'T1', toTier: 'T2' } }],
  ['user-5', { type: 'user.tier_changed', data: { fromTier: 'T2', toTier: 'T3' } }],
];

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ['localhost:9092'] } });

// idempotent: the broker rejects duplicate or out-of-order retries, so
// per-partition (and therefore per-user) order survives retries.
// acks -1 = wait for all in-sync replicas (required by idempotence).
const producer = kafka.producer({ kafkaJS: { idempotent: true, acks: -1 } });

await producer.connect();

for (const [userId, body] of scenario) {
  const event: LedgerEvent = {
    eventId: randomUUID(),
    userId,
    occurredAt: new Date().toISOString(),
    ...body,
  };

  // Key = userId → hash(userId) % 3 picks the partition.
  // Awaiting each send defeats batching, but makes the log below readable.
  const [meta] = await producer.send({
    topic: TOPIC,
    messages: [{ key: userId, value: JSON.stringify(event) }],
  });

  console.log(
    `${userId}  ${event.type.padEnd(18)} → partition ${meta.partition}, offset ${meta.baseOffset ?? meta.offset}`,
  );
}

await producer.disconnect();

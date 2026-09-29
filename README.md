# Ledger

A hands-on Kafka demo. User activity — orders, payments, tier changes — is
recorded as a **permanent, ordered event log**. Two independent readers
process that log at their own pace, one live and one in batches, and the log
can be **replayed** to rebuild state from scratch.

Ledger is the companion to [Bouncer](https://github.com/ilker-ekin/Bouncer),
which uses RabbitMQ for real-time request prioritization. The two projects
exist to contrast the models:

| | Bouncer (RabbitMQ) | Ledger (Kafka) |
|---|---|---|
| A message is… | a request waiting to be handled | a fact that already happened |
| After it's read | deleted | kept on the log |
| Ordering | by priority | strict, per partition key |
| Multiple readers | each needs its own copy (fanout) | one copy, each group keeps its own position |
| Re-reading old messages | impossible | replay: move a group's position back |

## How it works

```
 producer                 Kafka topic: ledger-events           consumer groups
 (src/producer.ts)        (3 partitions, key = userId)
                          ┌────────────────────────┐
 order.created    ──────► │ partition 0            │ ──► live   — always running, per-user view
 payment.processed        │ partition 1            │
 user.tier_changed        │ partition 2            │ ──► batch  — runs occasionally, hourly rollup
                          └────────────────────────┘
                          events stay after being read
```

- **Producer** publishes JSON events keyed by `userId`. The same user always
  lands on the same partition, so each user's events stay in order.
- **live** consumer keeps a view per user (orders, total paid, tier) in a
  local SQLite file (`data/live.db`). Each event updates the view *and* the
  saved position in one transaction, so the two can never disagree — the view
  survives restarts and rebalances, and a redelivered event is never applied
  twice.
- **batch** consumer starts, reads everything since its last run, adds it to
  stored hourly totals (`data/batch.db`), and exits — like a cron job. The
  totals and the position they cover are saved in one transaction per run,
  so a crash can never make it count an event twice.
- Each **consumer group** tracks its own position (offset), so the two never
  affect each other. Stop one, and the other keeps going; the stopped one
  catches up later from where it left off.

Every event has the same envelope:

```json
{
  "eventId": "7f3c9a2e-…",
  "type": "payment.processed",
  "userId": "user-3",
  "occurredAt": "2026-09-28T10:15:00.000Z",
  "data": { "orderId": "order-3", "amountCents": 12000 }
}
```

## Project structure

```
docker-compose.yml       single Kafka broker (KRaft mode, bound to localhost)
src/producer.ts          publishes a fixed scenario of 14 events for 6 users
src/live-consumer.ts     group "live": per-user view, updated on every event
src/batch-consumer.ts    group "batch": hourly totals, runs until caught up
src/lag.ts               prints each group's lag (optionally every 2s)
data/                    SQLite state for live and batch (git-ignored, created on first run)
```

## Running it

**Requirements:** Docker, and Node.js 24+ (TypeScript runs directly through
Node's built-in type stripping — there is no build step).

```bash
# 1. Start a single Kafka broker (KRaft mode, localhost only)
docker compose up -d

# 2. Create the topic (auto-creation is disabled on purpose; safe to re-run)
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic ledger-events --partitions 3 --replication-factor 1

# 3. Install dependencies
npm install
```

npm 11 may warn that the Kafka client's install script was not run. That's
fine — the package ships a prebuilt native binary.

Node runs the TypeScript files without checking their types, so check them
separately after making changes:

```bash
npm run typecheck
```

## The three demos

### 1. Same key, same partition

```bash
node src/producer.ts
```

Publishes 14 events for 6 users and prints where each landed. Every event for
a given user shows the same partition.

### 2. Independent consumer groups

```bash
# Terminal A — keep running
node src/live-consumer.ts

# Terminal B
node src/producer.ts       # live prints the new events immediately
node src/producer.ts       # batch isn't running, so it falls behind
node src/batch-consumer.ts # batch processes exactly what it missed, then exits
```

Check each group's position and lag at any time:

```bash
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group batch
```

To see why saving state and position together matters, make batch crash
halfway:

```bash
CRASH_BEFORE_COMMIT=1 node src/batch-consumer.ts  # reads everything, then crashes
node src/batch-consumer.ts                        # processes the same events, once
```

The crash happens before batch's transaction commits, so SQLite rolls it
back — neither the totals nor the position are saved, and nothing is counted
twice. Wait about 30 seconds between the two commands (see
[Troubleshooting](#troubleshooting)).

### 3. Replay

Events stay on the log after being read, so a group can go back and read them
again — only its bookmark (committed offset) moves; nothing is re-sent. Try it
with a throwaway group:

```bash
# Read everything as group "replay-demo" — it saves a bookmark at the end
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic ledger-events --group replay-demo --from-beginning --timeout-ms 5000

# Run the same command again: nothing — the group resumes from its bookmark

# Move the bookmark back to the start (drop --execute for a dry run)
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group replay-demo --topic ledger-events --reset-offsets --to-earliest --execute

# Run the read command again: every event comes back
```

live and batch work differently: each stores its own position in SQLite next
to its state, so resetting their Kafka offsets has no effect. For them, replay
means rebuilding the derived state — delete the database, and on the next
start they find no saved position and replay every partition from the
beginning:

```bash
rm data/live.db    # live rebuilds every user's view
rm data/batch.db   # batch rebuilds every hourly total
```

## Monitoring lag

**Lag** is how far behind a consumer group is: the end of the log minus the
group's saved position, per partition. Kafka doesn't store it — it's
calculated on demand.

```bash
node src/lag.ts           # print each group's lag once
node src/lag.ts --watch   # redraw every 2 seconds (Ctrl+C to stop)
```

```
 group    p0    p1    p2 total
  live     0     0     0     0
 batch     0    25    45    70
```

The same number means different things per group: growing lag is normal for
batch between runs, but lag that stays above zero for live means it's stuck or
has crashed.

## Stopping and cleaning up

```bash
docker compose stop      # stop the broker, keep all events and group positions
docker compose up -d     # start it again later

docker compose down -v   # delete the broker and every event
rm -r data/              # delete live's and batch's saved state
```

After `down -v`, repeat the setup steps — the topic has to be created again.
Delete `data/` as well, or live and batch will try to resume from positions
on a log that no longer exists.

## Troubleshooting

- **Warnings like `Connection to node -1 … could not be established` right
  after `docker compose up -d`.** The broker takes 10–20 seconds to start.
  The command keeps retrying and succeeds once the broker is ready; if it
  gives up, run it again.
- **`Topic 'ledger-events' already exists`.** The topic was created on an
  earlier run and is still there. Nothing to do — the setup command above
  uses `--if-not-exists` to skip this.
- **A consumer starts but prints nothing for ~30 seconds after a crash.** A
  consumer that was killed (not stopped with Ctrl+C) stays registered in its
  group until its session times out (30 seconds for this client). Its
  partitions are handed out only after that. Stop consumers with Ctrl+C to
  avoid the wait.
- **`--reset-offsets` fails because the group is active.** Offsets can only
  be reset while no consumer in that group is running. Stop it first.

## Design decisions

| Decision | Choice | Why |
|---|---|---|
| Broker | Confluent `cp-kafka` in KRaft mode, single node | Real Kafka, including its own consensus — no ZooKeeper |
| Partitions | 3 | Smallest count that shows key routing and consumer parallelism |
| Partition key | `userId` | Keeps each user's order → payment → tier change in order |
| Event format | Plain JSON envelope | One small schema; no registry needed |
| Client | `@confluentinc/kafka-javascript` | Maintained, and partitions keys the same way the Java client does |
| Producer | Idempotent, `acks=all` | Retries can't duplicate or reorder events |
| Delivery | At-least-once | Nothing is lost; Kafka may deliver an event twice |
| Consumer state | SQLite, saved in the same transaction as the consumer's position | State and position can't disagree, so each event takes effect exactly once |
| New groups start at | The beginning of the log | The history is the point |

## Deliberately out of scope

Schema registry, exactly-once transactions, multiple brokers, security
(TLS/SASL/ACLs — the broker is bound to localhost instead), consumer state
shared across machines, and metrics dashboards. This is a learning project on
a single local broker.

## License

[MIT](LICENSE)

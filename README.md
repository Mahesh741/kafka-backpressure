# Backpressure

[![CI](https://github.com/Mahesh741/kafka-backpressure/actions/workflows/ci.yml/badge.svg)](https://github.com/Mahesh741/kafka-backpressure/actions/workflows/ci.yml)

Memory-aware admission control for a message consumer, in plain Java 17 with no dependencies.

**Live simulator:** https://mahesh741.github.io/demos/backpressure/

A consumer that takes everything it polls into memory is fine until the downstream system slows
down. Then in-flight messages pile up on the heap, the JVM dies with an `OutOfMemoryError`, every
uncommitted message is redelivered, and the restarted pod walks straight into a bigger backlog.

This library puts a gate in front of the consumer. A message is taken into memory only if three
checks pass; otherwise it stays in the broker and the overload shows up as consumer lag.

| Check | Class | What it prevents |
|---|---|---|
| Adaptive concurrency (AIMD) | `AimdLimiter` | Sending the downstream more parallel calls than it can serve |
| Workflow quota | `BackpressureGate` | One slow workflow starving unrelated ones |
| Global byte budget | `ByteBudget` | Heap exhaustion, whatever the payload sizes are |

## Usage

```java
BackpressureGate gate = new BackpressureGate(
        BackpressureGate.Config.defaults(200 * 1024 * 1024), // 200 MB of in-flight messages
        HeapPressure.jvm());
FootprintEstimator footprint = new FootprintEstimator(2.5, 512);

Optional<BackpressureGate.Permit> permit =
        gate.tryAcquire(record.workflowId(), footprint.estimate(record.serializedSize()));

if (permit.isEmpty()) {
    consumer.pause(partitions);       // leave it in Kafka, try again on the next poll
    return;
}
try {
    downstream.send(record);
    permit.get().success();           // additive increase
} catch (TimeoutException | OverloadedException e) {
    permit.get().overloaded();        // multiplicative decrease
    throw e;
} finally {
    permit.get().close();             // no-op if already completed
}
```

## How the limit adapts

- **Additive increase.** Each clean completion adds `1 / limit`, so the limit rises by one per full
  round of successes. It only rises while it is nearly saturated: an idle system proves nothing
  about capacity.
- **Multiplicative decrease.** A timeout, a downstream rejection, or heap utilisation above the high
  watermark multiplies the limit by the decrease factor (0.7 by default).
- **Cooldown.** A burst of failures describes one moment of overload, so decreases are rate limited
  and a burst costs a single cut.

## Run the tests

```bash
./gradlew test
```

17 tests cover the limiter's arithmetic and bounds, the byte budget under 8-thread contention,
workflow isolation, heap-pressure decreases, idempotent permits, and a deterministic overload
simulation that checks in-flight bytes never exceed the budget while the limit settles just under
downstream capacity.

## Scope

Written from scratch as a public, simplified illustration of the approach. It is not production
code from any employer and it is not wired to a real Kafka client.

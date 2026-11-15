package io.github.mahesh741.backpressure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mahesh741.backpressure.BackpressureGate.Config;
import io.github.mahesh741.backpressure.BackpressureGate.Permit;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A deterministic, single-threaded model of a consumer feeding a downstream system that
 * can only handle {@code CAPACITY} concurrent requests. Anything above that times out.
 * The broker holds far more messages than fit in memory.
 */
class OverloadSimulationTest {

    private static final long MB = 1024 * 1024;
    private static final int CAPACITY = 12;
    private static final long MESSAGE_BYTES = 5 * MB;
    private static final long BUDGET = 200 * MB;

    @Test
    void memoryStaysBoundedAndTheLimitSettlesNearDownstreamCapacity() {
        Config config = new Config(1, 2, 64, 0.7, Duration.ZERO, BUDGET, 1.0, 0.8);
        BackpressureGate gate = new BackpressureGate(config, () -> 0.4);

        int backlog = 50_000;
        int delivered = 0;
        long peakBytes = 0;
        List<Integer> limitsLate = new ArrayList<>();
        Deque<Permit> inFlight = new ArrayDeque<>();

        for (int tick = 0; tick < 4_000 && backlog > 0; tick++) {
            // Admit as much as the gate allows; the rest stays in the broker.
            Optional<Permit> permit;
            while (backlog > 0 && (permit = gate.tryAcquire("orders", MESSAGE_BYTES)).isPresent()) {
                inFlight.add(permit.get());
                backlog--;
            }
            peakBytes = Math.max(peakBytes, gate.totalInFlightBytes());

            // The downstream copes with CAPACITY concurrent requests; beyond that it times out.
            boolean overloaded = inFlight.size() > CAPACITY;
            Iterator<Permit> it = inFlight.iterator();
            while (it.hasNext()) {
                Permit p = it.next();
                if (overloaded) {
                    p.overloaded();
                    backlog++; // redelivered later
                } else {
                    p.success();
                    delivered++;
                }
                it.remove();
            }
            if (tick >= 2_000) limitsLate.add(gate.snapshot("orders").concurrencyLimit());
        }

        assertTrue(peakBytes <= BUDGET, "in-flight bytes peaked at " + peakBytes / MB + " MB");
        assertTrue(delivered > 10_000, "the system kept making progress: " + delivered);
        assertEquals(0, gate.totalInFlightBytes());

        double average = limitsLate.stream().mapToInt(Integer::intValue).average().orElseThrow();
        int max = limitsLate.stream().mapToInt(Integer::intValue).max().orElseThrow();
        assertTrue(average >= CAPACITY * 0.6 && average <= CAPACITY * 1.1,
                "limit should hover just under capacity " + CAPACITY + " but averaged " + average);
        assertTrue(max <= CAPACITY + 1, "limit overshoots capacity by at most one probe, saw " + max);
    }
}

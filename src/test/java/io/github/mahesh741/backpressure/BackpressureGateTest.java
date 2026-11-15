package io.github.mahesh741.backpressure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mahesh741.backpressure.BackpressureGate.Config;
import io.github.mahesh741.backpressure.BackpressureGate.Permit;
import io.github.mahesh741.backpressure.BackpressureGate.Rejection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class BackpressureGateTest {

    private static final long MB = 1024 * 1024;
    private static final HeapPressure CALM = () -> 0.3;

    private static Config config(int initialConcurrency, long maxBytes, double workflowShare) {
        return new Config(1, initialConcurrency, 64, 0.5, Duration.ZERO, maxBytes, workflowShare, 0.8);
    }

    @Test
    void enforcesTheConcurrencyLimitPerWorkflow() {
        BackpressureGate gate = new BackpressureGate(config(2, 1_000 * MB, 1.0), CALM);

        assertTrue(gate.tryAcquire("orders", MB).isPresent());
        assertTrue(gate.tryAcquire("orders", MB).isPresent());
        assertTrue(gate.tryAcquire("orders", MB).isEmpty());

        assertEquals(1, gate.rejectionCount(Rejection.CONCURRENCY_LIMIT));
        assertEquals(2, gate.snapshot("orders").inFlight());
    }

    @Test
    void enforcesTheGlobalByteBudgetAcrossWorkflows() {
        BackpressureGate gate = new BackpressureGate(config(50, 100 * MB, 1.0), CALM);

        assertTrue(gate.tryAcquire("orders", 60 * MB).isPresent());
        assertTrue(gate.tryAcquire("invoices", 30 * MB).isPresent());
        assertTrue(gate.tryAcquire("shipments", 20 * MB).isEmpty(), "60 + 30 + 20 > 100");

        assertEquals(1, gate.rejectionCount(Rejection.BYTE_BUDGET));
        assertEquals(90 * MB, gate.totalInFlightBytes());
    }

    @Test
    void aSlowWorkflowCannotStarveAnUnrelatedOne() {
        // "sap-sync" is stuck behind a slow downstream and never completes anything.
        BackpressureGate gate = new BackpressureGate(config(50, 100 * MB, 0.6), CALM);

        int admittedSlow = 0;
        while (gate.tryAcquire("sap-sync", 10 * MB).isPresent()) admittedSlow++;

        assertEquals(6, admittedSlow, "capped at 60% of the byte budget");
        assertEquals(1, gate.rejectionCount(Rejection.WORKFLOW_QUOTA));

        // The healthy workflow still gets through, because 40% of the budget was held back.
        for (int i = 0; i < 100; i++) {
            Optional<Permit> permit = gate.tryAcquire("order-events", 10 * MB);
            assertTrue(permit.isPresent(), "healthy workflow blocked on iteration " + i);
            permit.get().success();
        }
        assertEquals(6, gate.snapshot("sap-sync").inFlight());
    }

    @Test
    void heapPressureShrinksTheLimitEvenWhenTheDownstreamIsHealthy() {
        AtomicReference<Double> heap = new AtomicReference<>(0.3);
        BackpressureGate gate = new BackpressureGate(config(16, 1_000 * MB, 1.0), heap::get);

        gate.tryAcquire("orders", MB).orElseThrow().success();
        assertEquals(16, gate.snapshot("orders").concurrencyLimit());

        heap.set(0.92);
        gate.tryAcquire("orders", MB).ifPresent(Permit::success);

        assertEquals(8, gate.snapshot("orders").concurrencyLimit());
    }

    @Test
    void overloadSignalsCutTheLimitAndSuccessesRebuildIt() {
        BackpressureGate gate = new BackpressureGate(config(8, 1_000 * MB, 1.0), CALM);

        gate.tryAcquire("orders", MB).orElseThrow().overloaded();
        assertEquals(4, gate.snapshot("orders").concurrencyLimit());

        // Keep the limit saturated and completing cleanly: it climbs back.
        for (int round = 0; round < 40; round++) {
            List<Permit> batch = new ArrayList<>();
            Optional<Permit> next;
            while ((next = gate.tryAcquire("orders", MB)).isPresent()) batch.add(next.get());
            batch.forEach(Permit::success);
        }
        assertTrue(gate.snapshot("orders").concurrencyLimit() > 8,
                "limit should have recovered past its starting point");
    }

    @Test
    void completingAPermitTwiceReleasesOnce() {
        BackpressureGate gate = new BackpressureGate(config(4, 100 * MB, 1.0), CALM);
        Permit permit = gate.tryAcquire("orders", 10 * MB).orElseThrow();

        permit.success();
        permit.success();
        permit.overloaded();
        permit.close();

        assertEquals(0, gate.totalInFlightBytes());
        assertEquals(0, gate.snapshot("orders").inFlight());
        assertEquals(4, gate.snapshot("orders").concurrencyLimit(), "the late overloaded() call was ignored");
    }
}

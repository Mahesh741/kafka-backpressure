package io.github.mahesh741.backpressure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ByteBudgetTest {

    @Test
    void admitsUntilTheBudgetIsFullThenRejects() {
        ByteBudget budget = new ByteBudget(100);

        assertTrue(budget.tryAcquire(60));
        assertTrue(budget.tryAcquire(40));
        assertFalse(budget.tryAcquire(1));

        budget.release(40);
        assertTrue(budget.tryAcquire(30));
        assertEquals(90, budget.inFlightBytes());
    }

    @Test
    void anOversizedMessageIsAdmittedOnlyWhenAlone() {
        ByteBudget budget = new ByteBudget(100);

        assertTrue(budget.tryAcquire(10));
        assertFalse(budget.tryAcquire(500), "must wait for the budget to drain");

        budget.release(10);
        assertTrue(budget.tryAcquire(500), "processed alone instead of starving forever");
        assertFalse(budget.tryAcquire(1), "nothing else rides along with it");
    }

    @Test
    void releasingMoreThanAcquiredIsABug() {
        ByteBudget budget = new ByteBudget(100);
        assertThrows(IllegalStateException.class, () -> budget.release(1));
    }

    @Test
    void neverExceedsTheBudgetUnderContention() throws Exception {
        long max = 10_000;
        ByteBudget budget = new ByteBudget(max);
        AtomicLong peak = new AtomicLong();
        AtomicLong admitted = new AtomicLong();
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                start.await();
                ThreadLocalRandom random = ThreadLocalRandom.current();
                for (int i = 0; i < 50_000; i++) {
                    long bytes = random.nextLong(1, 2_000);
                    if (budget.tryAcquire(bytes)) {
                        admitted.incrementAndGet();
                        peak.accumulateAndGet(budget.inFlightBytes(), Math::max);
                        budget.release(bytes);
                    }
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertTrue(admitted.get() > 0);
        assertTrue(peak.get() <= max, "peak in-flight bytes " + peak.get() + " exceeded budget " + max);
        assertEquals(0, budget.inFlightBytes(), "everything acquired was released");
    }
}

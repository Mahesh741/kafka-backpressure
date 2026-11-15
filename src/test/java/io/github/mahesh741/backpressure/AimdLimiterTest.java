package io.github.mahesh741.backpressure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class AimdLimiterTest {

    private final AtomicLong clock = new AtomicLong();

    private AimdLimiter limiter(int min, int initial, int max, double factor, Duration cooldown) {
        return new AimdLimiter(min, initial, max, factor, cooldown, clock::get);
    }

    private void advance(Duration d) {
        clock.addAndGet(d.toNanos());
    }

    @Test
    void increasesByOnePerFullRoundOfSuccesses() {
        AimdLimiter limiter = limiter(1, 10, 100, 0.5, Duration.ZERO);

        for (int i = 0; i < 9; i++) limiter.onSuccess(10);
        assertEquals(10, limiter.limit(), "nine successes are not yet a full round of ten");

        limiter.onSuccess(10);
        limiter.onSuccess(10);
        assertEquals(11, limiter.limit(), "additive increase: +1 after roughly one round");
    }

    @Test
    void doesNotGrowWhileTheLimitIsUnused() {
        AimdLimiter limiter = limiter(1, 10, 100, 0.5, Duration.ZERO);

        for (int i = 0; i < 1_000; i++) limiter.onSuccess(2);

        assertEquals(10, limiter.limit(), "an idle system proves nothing about capacity");
    }

    @Test
    void cutsMultiplicativelyOnOverload() {
        AimdLimiter limiter = limiter(1, 40, 100, 0.5, Duration.ZERO);

        assertTrue(limiter.onOverload());
        assertEquals(20, limiter.limit());
        assertTrue(limiter.onOverload());
        assertEquals(10, limiter.limit());
    }

    @Test
    void neverLeavesItsBounds() {
        AimdLimiter limiter = limiter(2, 4, 6, 0.5, Duration.ZERO);

        for (int i = 0; i < 10; i++) limiter.onOverload();
        assertEquals(2, limiter.limit(), "floor at minLimit");
        assertFalse(limiter.onOverload(), "no change is reported once at the floor");

        for (int i = 0; i < 10_000; i++) limiter.onSuccess(limiter.limit());
        assertEquals(6, limiter.limit(), "cap at maxLimit");
    }

    @Test
    void aBurstOfFailuresInsideTheCooldownCostsOneCut() {
        AimdLimiter limiter = limiter(1, 64, 100, 0.5, Duration.ofMillis(500));

        assertTrue(limiter.onOverload());
        for (int i = 0; i < 50; i++) assertFalse(limiter.onOverload());
        assertEquals(32, limiter.limit());

        advance(Duration.ofMillis(500));
        assertTrue(limiter.onOverload());
        assertEquals(16, limiter.limit());
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> limiter(0, 1, 10, 0.5, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> limiter(5, 1, 10, 0.5, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> limiter(1, 1, 10, 1.0, Duration.ZERO));
    }
}

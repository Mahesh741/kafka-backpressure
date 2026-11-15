package io.github.mahesh741.backpressure;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Adaptive concurrency limit using AIMD (additive increase, multiplicative decrease),
 * the same control loop TCP uses for congestion.
 *
 * <p>While work completes cleanly and the limit is actually being used, the limit creeps
 * up by one per "round" (one full limit's worth of successes). On an overload signal it is
 * cut by a constant factor. The result is a sawtooth that hovers just under whatever the
 * downstream system can really sustain, without anyone configuring that number.
 */
public final class AimdLimiter {

    private final int minLimit;
    private final int maxLimit;
    private final double decreaseFactor;
    private final long cooldownNanos;
    private final LongSupplier nanoClock;

    private double limit;
    private long lastDecreaseNanos;
    private boolean decreasedOnce;

    public AimdLimiter(int minLimit, int initialLimit, int maxLimit, double decreaseFactor, Duration cooldown) {
        this(minLimit, initialLimit, maxLimit, decreaseFactor, cooldown, System::nanoTime);
    }

    AimdLimiter(int minLimit, int initialLimit, int maxLimit, double decreaseFactor, Duration cooldown,
                LongSupplier nanoClock) {
        if (minLimit < 1) throw new IllegalArgumentException("minLimit must be >= 1");
        if (maxLimit < minLimit) throw new IllegalArgumentException("maxLimit must be >= minLimit");
        if (initialLimit < minLimit || initialLimit > maxLimit) {
            throw new IllegalArgumentException("initialLimit must be within [minLimit, maxLimit]");
        }
        if (decreaseFactor <= 0 || decreaseFactor >= 1) {
            throw new IllegalArgumentException("decreaseFactor must be in (0, 1)");
        }
        this.minLimit = minLimit;
        this.maxLimit = maxLimit;
        this.decreaseFactor = decreaseFactor;
        this.cooldownNanos = cooldown.toNanos();
        this.nanoClock = nanoClock;
        this.limit = initialLimit;
    }

    /** Current concurrency limit, always within [minLimit, maxLimit]. */
    public synchronized int limit() {
        return (int) limit;
    }

    /**
     * Record a clean completion.
     *
     * @param inFlightAtCompletion how many requests were in flight when this one finished,
     *                             including itself. The limit only grows when it is nearly
     *                             saturated: an idle system proves nothing about capacity.
     */
    public synchronized void onSuccess(int inFlightAtCompletion) {
        if (inFlightAtCompletion + 1 < (int) limit) {
            return;
        }
        limit = Math.min(maxLimit, limit + 1.0 / limit);
    }

    /**
     * Record an overload signal: a timeout, a downstream error, or heap pressure.
     * Decreases are rate limited by the cooldown so one burst of failures, which all
     * describe the same moment of overload, costs a single cut instead of collapsing
     * the limit to the minimum.
     *
     * @return true if the limit was actually reduced
     */
    public synchronized boolean onOverload() {
        long now = nanoClock.getAsLong();
        if (decreasedOnce && now - lastDecreaseNanos < cooldownNanos) {
            return false;
        }
        decreasedOnce = true;
        lastDecreaseNanos = now;
        double next = Math.max(minLimit, limit * decreaseFactor);
        boolean changed = next < limit;
        limit = next;
        return changed;
    }
}

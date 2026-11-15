package io.github.mahesh741.backpressure;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Admission control for outbound message processing.
 *
 * <p>A consumer asks the gate before it takes a message into memory. If the gate says no,
 * the message stays in the broker (the consumer pauses and retries later), so overload
 * shows up as consumer lag, which is cheap and durable, instead of as heap growth, which
 * ends in an OutOfMemoryError.
 *
 * <p>Three independent checks must all pass:
 * <ol>
 *   <li><b>Adaptive concurrency</b> per workflow, tuned by {@link AimdLimiter}.</li>
 *   <li><b>Workflow quota</b>: no single workflow may hold more than its share of the
 *       byte budget, so one slow integration cannot starve the others.</li>
 *   <li><b>Global byte budget</b> across all workflows, see {@link ByteBudget}.</li>
 * </ol>
 */
public final class BackpressureGate {

    /** Why a message was not admitted. */
    public enum Rejection { CONCURRENCY_LIMIT, WORKFLOW_QUOTA, BYTE_BUDGET }

    public record Config(
            int minConcurrency,
            int initialConcurrency,
            int maxConcurrency,
            double decreaseFactor,
            Duration decreaseCooldown,
            long maxInFlightBytes,
            double maxWorkflowShare,
            double heapHighWatermark) {

        public Config {
            if (maxWorkflowShare <= 0 || maxWorkflowShare > 1) {
                throw new IllegalArgumentException("maxWorkflowShare must be in (0, 1]");
            }
            if (heapHighWatermark <= 0 || heapHighWatermark > 1) {
                throw new IllegalArgumentException("heapHighWatermark must be in (0, 1]");
            }
        }

        public static Config defaults(long maxInFlightBytes) {
            return new Config(1, 4, 64, 0.7, Duration.ofMillis(500), maxInFlightBytes, 0.6, 0.8);
        }
    }

    public record Snapshot(int concurrencyLimit, int inFlight, long inFlightBytes) { }

    private final Config config;
    private final HeapPressure heapPressure;
    private final ByteBudget globalBudget;
    private final long workflowByteCap;
    private final Map<String, WorkflowState> workflows = new ConcurrentHashMap<>();
    private final Map<Rejection, AtomicLong> rejections = new ConcurrentHashMap<>();

    public BackpressureGate(Config config, HeapPressure heapPressure) {
        this.config = config;
        this.heapPressure = heapPressure;
        this.globalBudget = new ByteBudget(config.maxInFlightBytes());
        this.workflowByteCap = (long) (config.maxInFlightBytes() * config.maxWorkflowShare());
        for (Rejection r : Rejection.values()) {
            rejections.put(r, new AtomicLong());
        }
    }

    /**
     * Ask to take one message into memory.
     *
     * @return a permit that must be completed exactly once, or empty if the message
     *         should stay in the broker for now
     */
    public Optional<Permit> tryAcquire(String workflowId, long estimatedBytes) {
        WorkflowState state = workflows.computeIfAbsent(workflowId, id -> new WorkflowState(newLimiter()));

        if (heapPressure.utilization() >= config.heapHighWatermark()) {
            state.limiter.onOverload();
        }

        synchronized (state) {
            if (state.inFlight >= state.limiter.limit()) {
                return reject(Rejection.CONCURRENCY_LIMIT);
            }
            boolean workflowHasWork = state.inFlightBytes > 0;
            if (workflowHasWork && state.inFlightBytes + estimatedBytes > workflowByteCap) {
                return reject(Rejection.WORKFLOW_QUOTA);
            }
            if (!globalBudget.tryAcquire(estimatedBytes)) {
                return reject(Rejection.BYTE_BUDGET);
            }
            state.inFlight++;
            state.inFlightBytes += estimatedBytes;
        }
        return Optional.of(new Permit(state, estimatedBytes));
    }

    public Snapshot snapshot(String workflowId) {
        WorkflowState state = workflows.get(workflowId);
        if (state == null) {
            return new Snapshot(config.initialConcurrency(), 0, 0);
        }
        synchronized (state) {
            return new Snapshot(state.limiter.limit(), state.inFlight, state.inFlightBytes);
        }
    }

    public long totalInFlightBytes() {
        return globalBudget.inFlightBytes();
    }

    public long rejectionCount(Rejection reason) {
        return rejections.get(reason).get();
    }

    private Optional<Permit> reject(Rejection reason) {
        rejections.get(reason).incrementAndGet();
        return Optional.empty();
    }

    private AimdLimiter newLimiter() {
        return new AimdLimiter(config.minConcurrency(), config.initialConcurrency(), config.maxConcurrency(),
                config.decreaseFactor(), config.decreaseCooldown());
    }

    private static final class WorkflowState {
        final AimdLimiter limiter;
        int inFlight;
        long inFlightBytes;

        WorkflowState(AimdLimiter limiter) {
            this.limiter = limiter;
        }
    }

    /** Held while a message is in memory. Complete it exactly once; later calls are ignored. */
    public final class Permit implements AutoCloseable {
        private final WorkflowState state;
        private final long bytes;
        private final AtomicBoolean done = new AtomicBoolean();

        private Permit(WorkflowState state, long bytes) {
            this.state = state;
            this.bytes = bytes;
        }

        /** The message was delivered downstream. */
        public void success() {
            int inFlightAtCompletion = release();
            if (inFlightAtCompletion >= 0) {
                state.limiter.onSuccess(inFlightAtCompletion);
            }
        }

        /** The downstream timed out or signalled overload: back off. */
        public void overloaded() {
            if (release() >= 0) {
                state.limiter.onOverload();
            }
        }

        /** Release without teaching the limiter anything, for example on a validation error. */
        @Override
        public void close() {
            release();
        }

        private int release() {
            if (!done.compareAndSet(false, true)) {
                return -1;
            }
            int before;
            synchronized (state) {
                before = state.inFlight;
                state.inFlight--;
                state.inFlightBytes -= bytes;
            }
            globalBudget.release(bytes);
            return before;
        }
    }
}

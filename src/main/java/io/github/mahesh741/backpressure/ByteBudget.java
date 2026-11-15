package io.github.mahesh741.backpressure;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A lock-free cap on the estimated bytes held in memory by in-flight messages.
 *
 * <p>Counting messages is not enough when payload sizes vary by orders of magnitude:
 * fifty 2 KB messages and fifty 80 MB messages are very different loads. Admission is
 * therefore decided in bytes.
 */
public final class ByteBudget {

    private final long maxBytes;
    private final AtomicLong inFlightBytes = new AtomicLong();

    public ByteBudget(long maxBytes) {
        if (maxBytes <= 0) throw new IllegalArgumentException("maxBytes must be positive");
        this.maxBytes = maxBytes;
    }

    /**
     * Try to reserve {@code bytes}. A message larger than the whole budget is admitted
     * only when nothing else is in flight, so an oversized payload is processed alone
     * rather than starving forever.
     */
    public boolean tryAcquire(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("bytes must be >= 0");
        while (true) {
            long current = inFlightBytes.get();
            boolean fits = current + bytes <= maxBytes;
            boolean aloneAndOversized = current == 0;
            if (!fits && !aloneAndOversized) {
                return false;
            }
            if (inFlightBytes.compareAndSet(current, current + bytes)) {
                return true;
            }
        }
    }

    public void release(long bytes) {
        long after = inFlightBytes.addAndGet(-bytes);
        if (after < 0) {
            throw new IllegalStateException("released more bytes than were acquired");
        }
    }

    public long inFlightBytes() {
        return inFlightBytes.get();
    }

    public long maxBytes() {
        return maxBytes;
    }
}

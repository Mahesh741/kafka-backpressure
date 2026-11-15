package io.github.mahesh741.backpressure;

/**
 * Estimates how much heap a message will occupy while it is in flight.
 *
 * <p>The wire size understates the real cost: a UTF-8 payload decoded to a Java String
 * and then parsed into an object tree typically occupies a multiple of its serialized
 * size. The estimate is deliberately simple (linear) so it is cheap to compute on every
 * message and easy to calibrate from a heap dump.
 */
public final class FootprintEstimator {

    private final double expansionFactor;
    private final long fixedOverheadBytes;

    public FootprintEstimator(double expansionFactor, long fixedOverheadBytes) {
        if (expansionFactor < 1) throw new IllegalArgumentException("expansionFactor must be >= 1");
        if (fixedOverheadBytes < 0) throw new IllegalArgumentException("fixedOverheadBytes must be >= 0");
        this.expansionFactor = expansionFactor;
        this.fixedOverheadBytes = fixedOverheadBytes;
    }

    public long estimate(long serializedBytes) {
        return (long) Math.ceil(serializedBytes * expansionFactor) + fixedOverheadBytes;
    }
}

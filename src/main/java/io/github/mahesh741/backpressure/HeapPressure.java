package io.github.mahesh741.backpressure;

/** Source of the heap utilisation signal, from 0.0 (empty) to 1.0 (full). */
@FunctionalInterface
public interface HeapPressure {

    double utilization();

    /** Reads the live JVM heap. */
    static HeapPressure jvm() {
        Runtime runtime = Runtime.getRuntime();
        return () -> {
            long used = runtime.totalMemory() - runtime.freeMemory();
            return (double) used / runtime.maxMemory();
        };
    }
}

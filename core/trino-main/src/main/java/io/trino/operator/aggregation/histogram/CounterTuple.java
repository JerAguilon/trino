package io.trino.operator.aggregation.histogram;

public class CounterTuple implements Comparable<CounterTuple> {
    Comparable key;
    long count;

    public CounterTuple(
        Comparable key,
        long count
    ) {
        this.key = key;
        this.count = count;
    }

    @Override
    public int compareTo(CounterTuple other) {
        return this.key.compareTo(other.key);
    }
}

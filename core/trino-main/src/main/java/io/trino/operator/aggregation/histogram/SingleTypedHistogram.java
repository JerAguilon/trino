/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.aggregation.histogram;

import io.airlift.slice.Slice;
import io.trino.array.IntBigArray;
import io.trino.array.LongBigArray;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

public class SingleTypedHistogram
        implements TypedHistogram
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleTypedHistogram.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;

    private final int expectedSize;
    private int hashCapacity;
    private int maxFill;
    private int mask;

    private final Type type;
    private final BlockPositionEqual equalOperator;
    private final BlockPositionHashCode hashCodeOperator;
    private final BlockBuilder values;

    private IntBigArray hashPositions;
    private final LongBigArray counts;

    private SingleTypedHistogram(Type type, BlockPositionEqual equalOperator, BlockPositionHashCode hashCodeOperator, int expectedSize, int hashCapacity, BlockBuilder values)
    {
        this.type = requireNonNull(type, "type is null");
        this.equalOperator = requireNonNull(equalOperator, "equalOperator is null");
        this.hashCodeOperator = requireNonNull(hashCodeOperator, "hashCodeOperator is null");
        this.expectedSize = expectedSize;
        this.hashCapacity = hashCapacity;
        this.values = values;

        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        hashPositions = new IntBigArray(-1);
        hashPositions.ensureCapacity(hashCapacity);
        counts = new LongBigArray();
        counts.ensureCapacity(hashCapacity);
    }

    public SingleTypedHistogram(Type type, BlockPositionEqual equalOperator, BlockPositionHashCode hashCodeOperator, int expectedSize)
    {
        this(type,
                equalOperator,
                hashCodeOperator,
                expectedSize,
                computeBucketCount(expectedSize),
                type.createBlockBuilder(null, computeBucketCount(expectedSize)));
    }

    private static int computeBucketCount(int expectedSize)
    {
        return arraySize(expectedSize, FILL_RATIO);
    }

    public SingleTypedHistogram(Block block, Type type, BlockPositionEqual equalOperator, BlockPositionHashCode hashCodeOperator, int expectedSize)
    {
        this(type, equalOperator, hashCodeOperator, expectedSize);
        requireNonNull(block, "block is null");
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(i, block, BIGINT.getLong(block, i + 1));
        }
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + values.getRetainedSizeInBytes() + counts.sizeOf() + hashPositions.sizeOf();
    }

    @Override
    public void serialize(BlockBuilder out)
    {
        if (values.getPositionCount() == 0) {
            out.appendNull();
        }
        else {
            Block valuesBlock = values.build();
            BlockBuilder blockBuilder = out.beginBlockEntry();
            for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
                type.appendTo(valuesBlock, i, blockBuilder);
                BIGINT.writeLong(blockBuilder, counts.get(i));
            }
            out.closeEntry();
        }
    }

    private Comparable getObject(Block valuesBlock, int i) {
        Class<?> javaType = type.getJavaType();
        if (javaType.equals(boolean.class)) {
            return type.getBoolean(valuesBlock, i);
        }
        else if (javaType.equals(long.class)) {
            return type.getLong(valuesBlock, i);
        }
        else if (javaType.equals(double.class)) {
            return type.getDouble(valuesBlock, i);
        }
        else if (javaType.equals(Slice.class)) {
            return type.getSlice(valuesBlock, i);
        }
        return ((Comparable)type.getObjectValue(null, valuesBlock, i));
    }


    private void writeObject(BlockBuilder out, Comparable obj) {
        Class<?> javaType = type.getJavaType();
        if (javaType.equals(boolean.class)) {
            type.writeBoolean(out, (Boolean)obj);
        }
        else if (javaType.equals(long.class)) {
            type.writeLong(out, (Long) obj);
        }
        else if (javaType.equals(double.class)) {
            type.writeDouble(out, (Double) obj);
        }
        else if (javaType.equals(Slice.class)) {
            type.writeSlice(out, (Slice) obj);
        } else {
            type.writeObject(out, obj);
        }
    }

    @Override
    public void serializeMedian(BlockBuilder out, double percentile)
    {
        if (values.getPositionCount() == 0) {
            out.appendNull();
        }
        else {
            Block valuesBlock = values.build();
            CounterTuple[] counters = new CounterTuple[valuesBlock.getPositionCount()];
            Class<?> javaType = type.getJavaType();
            for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
                Comparable obj = getObject(valuesBlock, i);
                long count = counts.get(i);
                counters[i] = new CounterTuple(obj, count);
            }

            makeCountersCumulative(counters);
            Comparable value = bisect(counters, (int)(counters.length * percentile + 1)); // TODO: support non-medians
            writeObject(out, value);
        }
    }

    private long makeCountersCumulative(CounterTuple[] counters) {
        Arrays.sort(counters);
        long runningSum = 0;
        for (int i = 0; i < counters.length; i++) {
            CounterTuple curr = counters[i];
            runningSum += curr.count;
            curr.count = runningSum;
        }
        return runningSum;
    }
    private Comparable bisect(CounterTuple[] cumulativeCounters, long target) {
        CounterTuple dummyCounter = new CounterTuple(null, target);
        int index = Arrays.binarySearch(
                cumulativeCounters,
                dummyCounter,
                (o1, o2) -> {
                    long diff = o1.count - o2.count;
                    // Return -1/0/1 to avoid unsafe long conversions to int
                    if (diff > 0) {
                        return 1;
                    } else if (diff < 0) {
                        return -1;
                    }
                    return 0;
                }
        );
        if (index < 0) {
            index = 0;
        } else if (index > cumulativeCounters.length - 1) {
            index = cumulativeCounters.length - 1;
        }
        return cumulativeCounters[index].key;
    }

    @Override
    public void addAll(TypedHistogram other)
    {
        other.readAllValues((block, position, count) -> add(position, block, count));
    }

    @Override
    public void readAllValues(HistogramValueReader reader)
    {
        for (int i = 0; i < values.getPositionCount(); i++) {
            long count = counts.get(i);
            if (count > 0) {
                reader.read(values, i, count);
            }
        }
    }

    @Override
    public void add(int position, Block block, long count)
    {
        int hashPosition = getBucketId(hashCodeOperator.hashCodeNullSafe(block, position), mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            if (hashPositions.get(hashPosition) == -1) {
                break;
            }

            if (equalOperator.equal(block, position, values, hashPositions.get(hashPosition))) {
                counts.add(hashPositions.get(hashPosition), count);
                return;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        addNewGroup(hashPosition, position, block, count);
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public int getExpectedSize()
    {
        return expectedSize;
    }

    @Override
    public boolean isEmpty()
    {
        return values.getPositionCount() == 0;
    }

    private void addNewGroup(int hashPosition, int position, Block block, long count)
    {
        hashPositions.set(hashPosition, values.getPositionCount());
        counts.set(values.getPositionCount(), count);
        type.appendTo(block, position, values);

        // increase capacity, if necessary
        if (values.getPositionCount() >= maxFill) {
            rehash();
        }
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;

        int newMask = newCapacity - 1;
        IntBigArray newHashPositions = new IntBigArray(-1);
        newHashPositions.ensureCapacity(newCapacity);

        for (int i = 0; i < values.getPositionCount(); i++) {
            // find an empty slot for the address
            int hashPosition = getBucketId(hashCodeOperator.hashCodeNullSafe(values, i), newMask);

            while (newHashPositions.get(hashPosition) != -1) {
                hashPosition = (hashPosition + 1) & newMask;
            }

            // record the mapping
            newHashPositions.set(hashPosition, i);
        }

        hashCapacity = newCapacity;
        mask = newMask;
        maxFill = calculateMaxFill(newCapacity);
        hashPositions = newHashPositions;

        this.counts.ensureCapacity(maxFill);
    }

    private static int getBucketId(long rawHash, int mask)
    {
        return ((int) murmurHash3(rawHash)) & mask;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }
}

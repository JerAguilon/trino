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

import io.trino.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

/**
 * state object that uses a single histogram for all groups. See {@link GroupedTypedHistogram}
 */
public class GroupedHistogramState
        extends AbstractGroupedAccumulatorState
        implements HistogramState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedHistogramState.class).instanceSize();
    private final Type type;
    private final BlockPositionEqual equalOperator;
    private final BlockPositionHashCode hashCodeOperator;
    private TypedHistogram typedHistogram;
    private long size;

    public double percentile = 0.5;

    public GroupedHistogramState(Type type, BlockPositionEqual equalOperator, BlockPositionHashCode hashCodeOperator, int expectedEntriesCount)
    {
        this.type = requireNonNull(type, "type is null");
        this.equalOperator = requireNonNull(equalOperator, "equalOperator is null");
        this.hashCodeOperator = requireNonNull(hashCodeOperator, "hashCodeOperator is null");
        typedHistogram = new GroupedTypedHistogram(type, equalOperator, hashCodeOperator, expectedEntriesCount);
        this.percentile = 0.5;
    }

    @Override
    public void ensureCapacity(long size)
    {
        typedHistogram.ensureCapacity(size);
    }

    @Override
    public TypedHistogram get()
    {
        return typedHistogram.setGroupId(getGroupId());
    }

    @Override
    public void deserialize(Block block, int expectedSize)
    {
        typedHistogram = new GroupedTypedHistogram(getGroupId(), block, type, equalOperator, hashCodeOperator, expectedSize);
    }

    @Override
    public void addMemoryUsage(long memory)
    {
        size += memory;
    }

    @Override
    public void setPercentile(double percentile) {
        this.percentile = percentile;
    }

    @Override
    public double getPercentile() {
        return this.percentile;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + size + typedHistogram.getEstimatedSize();
    }
}

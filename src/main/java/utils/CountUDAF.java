package utils;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author yang 2021/10/19
 *
 */
public class CountUDAF extends AggregateFunction<Long, CountUDAF.CountAccum> {
    //define accumulator datastructures
    public static class CountAccum {
        public long total;
    }

    //initial accumulator。
    @Override
    public CountAccum createAccumulator() {
        CountAccum acc = new CountAccum();
        acc.total = 0;
        return acc;
    }

    @Override
    public Long getValue(CountAccum accumulator) {
        return accumulator.total;
    }

    public void accumulate(CountAccum accumulator, Double iValue) {
        accumulator.total++;
    }

    public void retract(CountAccum accumulator, Double iValue) {
        accumulator.total--;
    }
    public void merge(CountAccum accumulator, Iterable<CountAccum> its) {
        for (CountAccum other : its) {
            accumulator.total += other.total;
        }
    }
}
package org.ascension.addg.gcp.ingestion.core;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

import java.util.Objects;

/**
 * Custom transform to replicated Reshuffle transform
 * @param <I> type of values being shuffled
 */
public class Reshuffle<I> extends PTransform<@NonNull PCollection<KV<String, I>>, @NonNull PCollection<KV<String, I>>> {
    @Override
    @NonNull
    public PCollection<KV<String, I>> expand(PCollection<KV<String, I>> input) {
        return input
                .apply(Window.<KV<String, I>>into(new IdentityWindowFn<>(input.getWindowingStrategy().getWindowFn().windowCoder()))
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(3)))
                    .discardingFiredPanes()
                    .withTimestampCombiner(TimestampCombiner.EARLIEST)
                    .withAllowedLateness(Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()), Window.ClosingBehavior.FIRE_ALWAYS))
                .apply("ReifyOriginalTimestamps", Reify.timestampsInValue())
                .apply(GroupByKey.create())
                .setWindowingStrategyInternal(input.getWindowingStrategy())
                .apply("ExpandIterable", ParDo.of(new ExpandDoFn<I>()))
                .apply(Reify.extractTimestampsFromValues());
    }

    /**
     * Expands an iterable of timestamped values
     * @param <I> Type
     */
    private static class ExpandDoFn <I> extends DoFn<KV<String, Iterable<TimestampedValue<I>>>, KV<String, TimestampedValue<I>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            var input = Objects.requireNonNull(c.element());
            for (var value : Objects.requireNonNull(input.getValue())) {
                c.output(KV.of(input.getKey(), value));
            }
        }
    }
}
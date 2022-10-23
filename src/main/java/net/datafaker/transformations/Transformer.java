package net.datafaker.transformations;

import net.datafaker.sequence.FakeSequence;

public interface Transformer<IN, OUT> {
    OUT apply(IN input, Schema<IN, ?> schema);
    OUT generate(FakeSequence<IN> input, final Schema<IN, ?> schema);
    OUT generate(final Schema<IN, ?> schema, int limit);
}

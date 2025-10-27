package ulpgc.bigd.indexing_service;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class TokenizerBenchmark {

    private static final String sample = """
        It is a truth universally acknowledged, that a single man in possession
        of a good fortune, must be in want of a wife.
        """;

    @Benchmark
    public void tokenizeBenchmark() {
        Tokenizer.tokenize(sample);
    }
}

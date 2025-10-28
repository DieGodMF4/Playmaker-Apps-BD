package ulpgc.bigd.search_service;

import org.openjdk.jmh.annotations.*;
import java.util.concurrent.TimeUnit;
import java.util.List;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Benchmark)
public class SearchBenchmark {

    private SearchRepository repository;

    @Setup(Level.Trial)
    public void setup() {
        repository = new SearchRepository();
    }

    @Benchmark
    public List<Book> searchSimple() {
        return repository.search("adventure", null, null, null);
    }

    @Benchmark
    public List<Book> searchByAuthor() {
        return repository.search("adventure", "Jane Austen", null, null);
    }

    @Benchmark
    public List<Book> searchByLanguage() {
        return repository.search("adventure", null, "fr", null);
    }

    @Benchmark
    public List<Book> searchCombined() {
        return repository.search("adventure", "Jules Verne", "fr", 1865);
    }
}

package ulpgc.bigd.indexing_service;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

final class IndexRepository {

    private final Gson gson = new Gson();
    private final Path datalake;
    private final Path datamart;
    private final Path indexFile;
    private final Path metadataFile;
    private Map<String, Set<Integer>> invertedIndex;
    private Map<Integer, Map<String, Object>> metadata;

    IndexRepository() {
        this.datalake = Paths.get(System.getenv().getOrDefault("DATALAKE_DIR", "../ingestion-service/datalake")).toAbsolutePath();
        this.datamart = Paths.get(System.getenv().getOrDefault("DATAMART_DIR", "./datamart")).toAbsolutePath();
        this.indexFile = datamart.resolve("inverted-index.json");
        this.metadataFile = datamart.resolve("metadata.json");
        load();
    }

    private void load() {
        try {
            if (!Files.exists(datamart)) {
                Files.createDirectories(datamart);
            }
            Type idxType = new TypeToken<Map<String, Set<Integer>>>() {
            }.getType();
            Type metaType = new TypeToken<Map<Integer, Map<String, Object>>>() {
            }.getType();
            invertedIndex = gson.fromJson(Files.newBufferedReader(indexFile), idxType);
            metadata = gson.fromJson(Files.newBufferedReader(metadataFile), metaType);
            if (invertedIndex == null) {
                invertedIndex = new HashMap<>();
            }
            if (metadata == null) {
                metadata = new HashMap<>();
            }
        } catch (IOException e) {
            System.err.println("[WARN] Failed to load existing index files, starting fresh: " + e.getMessage());
            invertedIndex = new HashMap<>();
            metadata = new HashMap<>();
        }
    }

    void indexBook(int bookId) throws IOException {
        Path bookDir = findBookDir(bookId);
        if (bookDir == null) {
            throw new IOException("Book not found in datalake: " + bookId);
        }

        String body = Files.readString(bookDir.resolve("body.txt"));
        String header = Files.readString(bookDir.resolve("header.txt"));

        Map<String, Object> meta = MetadataExtractor.extract(header);
        metadata.put(bookId, meta);

        Set<String> tokens = Tokenizer.tokenize(body);
        for (String token : tokens) {
            invertedIndex.computeIfAbsent(token, k -> new HashSet<>()).add(bookId);
        }
        save();
    }

    int rebuild() throws IOException {
        invertedIndex.clear();
        metadata.clear();
        List<Path> books = Files.walk(datalake)
                .filter(p -> p.getFileName().toString().equals("body.txt"))
                .collect(Collectors.toList());
        for (Path body : books) {
            int bookId = Integer.parseInt(body.getParent().getFileName().toString());
            indexBook(bookId);
        }
        return metadata.size();
    }

    private void save() throws IOException {
        Files.writeString(indexFile, gson.toJson(invertedIndex), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(metadataFile, gson.toJson(metadata), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private Path findBookDir(int bookId) throws IOException {
        try (var stream = Files.walk(datalake, 4)) {
            return stream.filter(p -> p.getFileName().toString().equals(String.valueOf(bookId)))
                    .findFirst().orElse(null);
        }
    }

    Map<String, Object> status() {
        return Map.of(
                "books_indexed", metadata.size(),
                "index_size", invertedIndex.size(),
                "last_update", Instant.now().toString()
        );
    }
}

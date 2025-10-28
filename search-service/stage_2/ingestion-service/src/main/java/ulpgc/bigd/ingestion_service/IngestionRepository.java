package ulpgc.bigd.ingestion_service;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

final class IngestionRepository {

    private final Path datalakeRoot;
    private final Path logFile;
    private final Gson gson = new Gson();

    static final class Entry {

        private int bookId;
        private String path;

        Entry() {
        }

        Entry(int bookId, String path) {
            this.bookId = bookId;
            this.path = path;
        }

        int bookId() {
            return bookId;
        }

        String path() {
            return path;
        }
    }

    IngestionRepository() {
        String dl = System.getenv().getOrDefault("DATALAKE_DIR", "./datalake");
        this.datalakeRoot = Paths.get(dl).toAbsolutePath().normalize();
        this.logFile = datalakeRoot.resolve("ingestion-log.json");
        ensureDir(datalakeRoot);
        ensureFile(logFile);
    }

    Path resolveDatalakePathForNow() {
        ZoneId zone = Optional.ofNullable(System.getenv("TZ"))
                .map(ZoneId::of).orElse(ZoneId.systemDefault());
        ZonedDateTime now = ZonedDateTime.now(zone);
        String ymd = String.format("%04d%02d%02d", now.getYear(), now.getMonthValue(), now.getDayOfMonth());
        String hh = String.format("%02d", now.getHour());
        Path p = datalakeRoot.resolve(ymd).resolve(hh);
        ensureDir(p);
        return p;
    }

    Path persistBook(Path base, int bookId, GutenbergDownloader.Split split) throws IOException {
        Path bookDir = base.resolve(String.valueOf(bookId));
        ensureDir(bookDir);
        Files.writeString(bookDir.resolve("header.txt"), split.header(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(bookDir.resolve("body.txt"), split.body(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        return bookDir;
    }

    void markIngested(int bookId, String path) {
        Map<Integer, Entry> db = readDb();
        db.put(bookId, new Entry(bookId, path));
        writeDb(db);
    }

    Entry get(int bookId) {
        return readDb().get(bookId);
    }

    List<Integer> listIds() {
        return readDb().values().stream()
                .sorted(Comparator.comparingInt(Entry::bookId))
                .map(Entry::bookId)
                .collect(Collectors.toList());
    }

    private Map<Integer, Entry> readDb() {
        try (Reader r = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)) {
            Type t = new TypeToken<Map<Integer, Entry>>() {
            }.getType();
            Map<Integer, Entry> m = gson.fromJson(r, t);
            return (m == null) ? new LinkedHashMap<>() : new LinkedHashMap<>(m);
        } catch (IOException e) {
            return new LinkedHashMap<>();
        }
    }

    private void writeDb(Map<Integer, Entry> db) {
        try (Writer w = Files.newBufferedWriter(logFile, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            gson.toJson(db, w);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write ingestion log: " + e.getMessage(), e);
        }
    }

    private static void ensureDir(Path p) {
        try {
            Files.createDirectories(p);
        } catch (IOException ignored) {
        }
    }

    private static void ensureFile(Path f) {
        try {
            if (!Files.exists(f)) {
                Files.createDirectories(f.getParent());
                Files.writeString(f, "{}", StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot create log file " + f + ": " + e.getMessage(), e);
        }
    }
}

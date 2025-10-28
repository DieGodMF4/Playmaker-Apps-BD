package ulpgc.bigd.ingestion_service;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import io.javalin.http.Context;

public class ServiceController {

    private static final Gson gson = new Gson();
    private static final IngestionRepository repo = new IngestionRepository();

    public static void ingestBook(Context ctx) {
        int bookId = Integer.parseInt(ctx.pathParam("book_id"));
        try {
            Path base = repo.resolveDatalakePathForNow();
            GutenbergDownloader.Result dl = GutenbergDownloader.downloadBook(bookId);
            GutenbergDownloader.Split split = GutenbergDownloader.splitHeaderBody(dl.content());
            Path bookDir = repo.persistBook(base, bookId, split);
            repo.markIngested(bookId, bookDir.toString());

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("book_id", bookId);
            resp.put("status", "downloaded");
            resp.put("path", bookDir.toString());
            ctx.result(gson.toJson(resp));
        } catch (Exception e) {
            ctx.status(500).result(gson.toJson(Map.of(
                    "error", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "book_id", bookId
            )));
        }
    }

    public static void ingestStatus(Context ctx) {
        int bookId = Integer.parseInt(ctx.pathParam("book_id"));
        IngestionRepository.Entry entry = repo.get(bookId);
        if (entry == null) {
            ctx.status(404).result(gson.toJson(Map.of(
                    "book_id", bookId,
                    "status", "not_found"
            )));
            return;
        }
        ctx.result(gson.toJson(Map.of(
                "book_id", bookId,
                "status", "available",
                "path", entry.path()
        )));
    }

    public static void listBooks(Context ctx) {
        List<Integer> ids = repo.listIds();
        ctx.result(gson.toJson(Map.of(
                "count", ids.size(),
                "books", ids
        )));
    }
}

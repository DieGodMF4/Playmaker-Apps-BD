package ulpgc.bigd.indexing_service;

import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;

import io.javalin.http.Context;

public class ServiceController {

    private static final Gson gson = new Gson();
    private static final IndexRepository repo = new IndexRepository();

    public static void updateBook(Context ctx) {
        int bookId = Integer.parseInt(ctx.pathParam("book_id"));
        try {
            repo.indexBook(bookId);
            ctx.result(gson.toJson(Map.of("book_id", bookId, "index", "updated")));
        } catch (IOException e) {
            ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
        }
    }

    public static void rebuildIndex(Context ctx) {
        try {
            int count = repo.rebuild();
            ctx.result(gson.toJson(Map.of("books_processed", count, "elapsed_time", "ok")));
        } catch (IOException e) {
            ctx.status(500).result(gson.toJson(Map.of("error", e.getMessage())));
        }
    }

    public static void getStatus(Context ctx) {
        ctx.result(gson.toJson(repo.status()));
    }
}

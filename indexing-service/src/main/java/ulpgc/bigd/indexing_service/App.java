package ulpgc.bigd.indexing_service;

import java.util.Map;

import com.google.gson.Gson;

import io.javalin.Javalin;

public class App {

    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        int tmp = 7002;
        try {
            String env = System.getenv("PORT");
            if (env != null && !env.isBlank()) {
                tmp = Integer.parseInt(env.trim());
            }
        } catch (NumberFormatException e) {
            System.err.println("[WARN] Invalid PORT value, defaulting to " + tmp + ": " + e.getMessage());
        }
        final int port = tmp;

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json")
                .start(port);

        app.get("/status", ctx
                -> ctx.result(gson.toJson(Map.of("service", "indexing-service", "status", "running", "port", port)))
        );

        app.post("/index/update/{book_id}", ServiceController::updateBook);
        app.post("/index/rebuild", ServiceController::rebuildIndex);
        app.get("/index/status", ServiceController::getStatus);
    }
}

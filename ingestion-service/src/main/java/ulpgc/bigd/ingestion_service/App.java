package ulpgc.bigd.ingestion_service;

import java.util.Map;

import com.google.gson.Gson;

import io.javalin.Javalin;

public class App {

    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        // Compute port in a temp var, then assign to a final var to be "effectively final".
        int tmp = 7001;
        try {
            String envPort = System.getenv("PORT");
            if (envPort != null && !envPort.isBlank()) {
                tmp = Integer.parseInt(envPort.trim());
            }
        } catch (Exception ignored) {
        }
        final int port = tmp;

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json")
                .start(port);

        app.get("/status", ctx -> {
            Map<String, Object> status = Map.of(
                    "service", "ingestion-service",
                    "status", "running",
                    "port", port
            );
            ctx.result(gson.toJson(status));
        });

        app.post("/ingest/{book_id}", ServiceController::ingestBook);
        app.get("/ingest/status/{book_id}", ServiceController::ingestStatus);
        app.get("/ingest/list", ServiceController::listBooks);
    }
}

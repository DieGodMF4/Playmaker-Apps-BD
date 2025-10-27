package ulpgc.bigd.search_service;

import io.javalin.Javalin;
import com.google.gson.Gson;
import java.util.Map;

public class App {

    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        int port = 7003;

        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
            config.plugins.enableCors(cors -> cors.add(it -> it.anyHost())); // útil para pruebas desde navegador
        }).start(port);

        System.out.println("✅ Search Service running on port " + port);

        app.get("/status", ctx -> {
            Map<String, Object> status = Map.of(
                "service", "search-service",
                "status", "running",
                "port", port
            );
            ctx.result(gson.toJson(status));
        });

        // Endpoints de búsqueda
        app.get("/search", SearchController::search);
    }
}

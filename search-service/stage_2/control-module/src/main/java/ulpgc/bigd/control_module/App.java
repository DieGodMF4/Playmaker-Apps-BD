package ulpgc.bigd.control_module;

import io.javalin.Javalin;
import com.google.gson.Gson;

public class App {

    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        }).start(7003); // Puerto del control module

        ControlController controller = new ControlController();

        app.get("/control/status", ctx -> ctx.result(gson.toJson(controller.getSystemStatus())));
        app.post("/control/process/{book_id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("book_id"));
            ctx.result(gson.toJson(controller.processBook(bookId)));
        });

        System.out.println("✅ Control Module running on http://localhost:7003");
    }
}


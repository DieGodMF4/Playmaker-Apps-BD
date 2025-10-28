package ulpgc.bigd.control_module;

import java.util.Map;

public class ControlController {

    private final Orchestrator orchestrator = new Orchestrator();
    private final StateManager state = new StateManager();

    public Map<String, Object> processBook(int bookId) {
        try {
            Map<String, Object> ingestion = orchestrator.ingestBook(bookId);
            if (!"downloaded".equals(ingestion.get("status")))
                return Map.of("book_id", bookId, "status", "ingestion_failed");

            Map<String, Object> indexing = orchestrator.indexBook(bookId);
            if (!"updated".equals(indexing.get("index")))
                return Map.of("book_id", bookId, "status", "indexing_failed");

            state.updateState(bookId, "indexed");
            return Map.of("book_id", bookId, "status", "completed");
        } catch (Exception e) {
            e.printStackTrace();
            return Map.of("book_id", bookId, "status", "error", "message", e.getMessage());
        }
    }

    public Map<String, Object> getSystemStatus() {
        return state.getAllStates();
    }
}


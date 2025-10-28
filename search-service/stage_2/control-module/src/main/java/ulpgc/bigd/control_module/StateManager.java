package ulpgc.bigd.control_module;

import java.util.*;
import java.io.*;
import com.google.gson.Gson;

public class StateManager {

    private final Map<Integer, String> stateMap = new HashMap<>();
    private static final String FILE_PATH = "state.json";
    private static final Gson gson = new Gson();

    public StateManager() {
        loadState();
    }

    public void updateState(int bookId, String status) {
        stateMap.put(bookId, status);
        saveState();
    }

    public Map<String, Object> getAllStates() {
        return Map.of("books", stateMap, "count", stateMap.size());
    }

    private void loadState() {
        try {
            File file = new File(FILE_PATH);
            if (file.exists()) {
                String json = new String(java.nio.file.Files.readAllBytes(file.toPath()));
                Map<Integer, String> loaded = gson.fromJson(json, Map.class);
                if (loaded != null) stateMap.putAll(loaded);
            }
        } catch (Exception e) {
            System.err.println("⚠️ Error loading state: " + e.getMessage());
        }
    }

    private void saveState() {
        try (FileWriter fw = new FileWriter(FILE_PATH)) {
            fw.write(gson.toJson(stateMap));
        } catch (IOException e) {
            System.err.println("⚠️ Error saving state: " + e.getMessage());
        }
    }
}


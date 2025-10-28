package ulpgc.bigd.control_module;

import com.google.gson.Gson;
import java.net.http.*;
import java.net.URI;
import java.util.Map;

public class Orchestrator {

    private static final Gson gson = new Gson();
    private final HttpClient client = HttpClient.newHttpClient();

    private static final String INGESTION_URL = "http://localhost:7001";
    private static final String INDEXING_URL = "http://localhost:7002";

    public Map<String, Object> ingestBook(int bookId) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(INGESTION_URL + "/ingest/" + bookId))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        return gson.fromJson(res.body(), Map.class);
    }

    public Map<String, Object> indexBook(int bookId) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(INDEXING_URL + "/index/update/" + bookId))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        return gson.fromJson(res.body(), Map.class);
    }
}


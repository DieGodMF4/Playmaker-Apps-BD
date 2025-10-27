package ulpgc.bigd.search_service;

import io.javalin.http.Context;
import com.google.gson.Gson;
import java.util.*;

public class SearchController {

    private static final Gson gson = new Gson();

    public static void search(Context ctx) {
        String q = ctx.queryParam("q"); // término
        String author = ctx.queryParam("author");
        String language = ctx.queryParam("language");
        Integer year = null;
        try {
            String y = ctx.queryParam("year");
            if (y != null) year = Integer.parseInt(y);
        } catch (NumberFormatException ignored) {}

        List<Book> results = SearchRepository.search(q, author, language, year);

        Map<String, Object> response = new LinkedHashMap<>();
        Map<String, Object> filters = new LinkedHashMap<>();
        if (author != null) filters.put("author", author);
        if (language != null) filters.put("language", language);
        if (year != null) filters.put("year", year);

        response.put("query", q == null ? "" : q);
        response.put("filters", filters);
        response.put("count", results.size());
        response.put("results", results);

        ctx.result(gson.toJson(response));
    }
}

package ulpgc.bigd.indexing_service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class MetadataExtractor {

    private static final Pattern TITLE = Pattern.compile("(?i)^Title:\\s*(.*)$", Pattern.MULTILINE);
    private static final Pattern AUTHOR = Pattern.compile("(?i)^Author:\\s*(.*)$", Pattern.MULTILINE);
    private static final Pattern LANGUAGE = Pattern.compile("(?i)^Language:\\s*(.*)$", Pattern.MULTILINE);
    private static final Pattern YEAR = Pattern.compile("(\\b1[0-9]{3}\\b|\\b20[0-9]{2}\\b)");

    static Map<String, Object> extract(String header) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("title", matchFirst(TITLE, header));
        m.put("author", matchFirst(AUTHOR, header));
        m.put("language", matchFirst(LANGUAGE, header));
        m.put("year", matchFirst(YEAR, header));
        return m;
    }

    private static String matchFirst(Pattern p, String text) {
        Matcher m = p.matcher(text);
        return m.find() ? m.group(1).trim() : "";
    }
}

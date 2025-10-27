package ulpgc.bigd.indexing_service;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

final class Tokenizer {

    private static final Pattern SPLIT = Pattern.compile("[^a-zA-Z]+");

    static Set<String> tokenize(String text) {
        Set<String> tokens = new HashSet<>();
        for (String t : SPLIT.split(text.toLowerCase())) {
            if (t.length() > 2 && t.length() < 30) {
                tokens.add(t);
            }
        }
        return tokens;
    }
}

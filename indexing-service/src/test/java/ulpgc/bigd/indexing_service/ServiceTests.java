package ulpgc.bigd.indexing_service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class ServiceTests {

    @Test
    void tokenizerShouldSplitWords() {
        var tokens = Tokenizer.tokenize("Hello world, hello index!");
        assertTrue(tokens.contains("hello"));
        assertTrue(tokens.contains("world"));
    }

    @Test
    void metadataExtractorShouldFindAuthor() {
        String header = "Title: The Epstein Files\nAuthor: An Endangered Author\nLanguage: English\n";
        var meta = MetadataExtractor.extract(header);
        assertEquals("An Endangered Author", meta.get("author"));
    }
}

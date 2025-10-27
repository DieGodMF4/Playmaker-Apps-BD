package ulpgc.bigd.search_service;

import java.util.*;
import java.util.stream.Collectors;

public class SearchRepository {

    private static final List<Book> DATA = new ArrayList<>();

    static {
        // Dataset mínimo de ejemplo
        DATA.add(new Book(5,   "Robinson Crusoe", "Daniel Defoe",  "en", 1719));
        DATA.add(new Book(11,  "Alice’s Adventures in Wonderland", "Lewis Carroll", "en", 1865));
        DATA.add(new Book(12,  "De la Terre à la Lune", "Jules Verne", "fr", 1865));
        DATA.add(new Book(1342,"Pride and Prejudice", "Jane Austen", "en", 1813));
        DATA.add(new Book(158, "Emma", "Jane Austen", "en", 1815));
        DATA.add(new Book(201, "Sense and Sensibility", "Jane Austen", "en", 1811));
        DATA.add(new Book(4201,"Vingt mille lieues sous les mers", "Jules Verne", "fr", 1870));
        DATA.add(new Book(6500,"Les Misérables", "Victor Hugo", "fr", 1862));
    }

    public static List<Book> search(String term, String author, String language, Integer year) {
        String t = term == null ? "" : term.toLowerCase();
        String a = author == null ? "" : author.toLowerCase();
        String l = language == null ? "" : language.toLowerCase();

        return DATA.stream()
            .filter(b -> t.isEmpty() || b.title.toLowerCase().contains(t))
            .filter(b -> a.isEmpty() || b.author.toLowerCase().contains(a))
            .filter(b -> l.isEmpty() || b.language.equalsIgnoreCase(l))
            .filter(b -> year == null || b.year == year)
            .collect(Collectors.toList());
    }
}

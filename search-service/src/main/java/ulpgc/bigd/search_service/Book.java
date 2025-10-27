package ulpgc.bigd.search_service;

public class Book {
    public final int bookId;
    public final String title;
    public final String author;
    public final String language;
    public final int year;

    public Book(int bookId, String title, String author, String language, int year) {
        this.bookId = bookId;
        this.title = title;
        this.author = author;
        this.language = language;
        this.year = year;
    }
}

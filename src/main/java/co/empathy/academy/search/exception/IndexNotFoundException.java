package co.empathy.academy.search.exception;

public class IndexNotFoundException extends RuntimeException {

    public IndexNotFoundException(String indexName, Exception e) {
        super("Index  " + indexName + " doesn't exist.", e);
    }

}

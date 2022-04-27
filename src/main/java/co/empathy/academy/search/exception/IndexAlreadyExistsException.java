package co.empathy.academy.search.exception;

public class IndexAlreadyExistsException extends RuntimeException {

    public IndexAlreadyExistsException(String indexName, Exception e) {
        super("Index with name " + indexName + " already exists.", e);
    }
}

package co.empathy.academy.search.exception;

public class IndexAlreadyExistsException extends Exception {

    public IndexAlreadyExistsException(String indexName) {
        super("Index with name " + indexName + " already exists.");
    }
}

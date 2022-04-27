package co.empathy.academy.search.exception;

public class IndexNotFoundException extends Exception {

    public IndexNotFoundException(String indexName) {
        super("Index with name " + indexName + " already exists.");
    }

}

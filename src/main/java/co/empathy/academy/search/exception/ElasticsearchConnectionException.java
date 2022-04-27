package co.empathy.academy.search.exception;

public class ElasticsearchConnectionException extends Exception {
    public ElasticsearchConnectionException() {
        super("Unable to connect to ElasticSearch");
    }
}

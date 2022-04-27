package co.empathy.academy.search.exception;

public class ElasticsearchConnectionException extends RuntimeException {
    public ElasticsearchConnectionException(Exception e) {
        super("Unable to connect to ElasticSearch", e);
    }
}

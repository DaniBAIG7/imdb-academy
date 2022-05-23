package co.empathy.academy.search.exception;

public class InternalServerException extends RuntimeException {

    public InternalServerException(Exception e) {
        super("There was a problem processing your request", e);
    }

}

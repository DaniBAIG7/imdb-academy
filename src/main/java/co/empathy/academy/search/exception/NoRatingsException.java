package co.empathy.academy.search.exception;

public class NoRatingsException extends Exception{
    public NoRatingsException() {
        super("A biParse operation was invoked, but no ratings were found in the parser");
    }
}

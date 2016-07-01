package io.github.yeagy.bss;

public class BetterSqlException extends Exception{
    public BetterSqlException() {
        super();
    }

    public BetterSqlException(String message) {
        super(message);
    }

    public BetterSqlException(String message, Throwable cause) {
        super(message, cause);
    }

    public BetterSqlException(Throwable cause) {
        super(cause);
    }

    protected BetterSqlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

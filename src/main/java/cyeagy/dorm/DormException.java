package cyeagy.dorm;

public class DormException extends Exception{
    public DormException() {
        super();
    }

    public DormException(String message) {
        super(message);
    }

    public DormException(String message, Throwable cause) {
        super(message, cause);
    }

    public DormException(Throwable cause) {
        super(cause);
    }

    protected DormException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

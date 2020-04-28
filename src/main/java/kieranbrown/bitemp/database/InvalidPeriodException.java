package kieranbrown.bitemp.database;

public class InvalidPeriodException extends Throwable {
    public InvalidPeriodException(final String reason) {
        super(reason);
    }
}

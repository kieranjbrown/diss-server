package kieranbrown.bitemp.database;

public enum QueryEquality {
    EQUALS("="),
    GREATER_THAN(">"),
    LESS_THAN("<"),
    GREATER_THAN_EQUAL_TO(">="),
    LESS_THAN_EQUAL_TO("<="),
    NOT_EQUAL("<>");

    private String value;

    QueryEquality(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

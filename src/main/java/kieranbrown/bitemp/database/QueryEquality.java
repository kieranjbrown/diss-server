package kieranbrown.bitemp.database;

public enum QueryEquality {
    EQUALS("="),
    GREATER_THAN(">"),
    LESS_THAN("<");

    private String value;

    private QueryEquality(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

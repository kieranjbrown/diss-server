package kieranbrown.bitemp.database;

import static java.util.Objects.requireNonNull;

public class NotQueryFilter implements QueryFilter {
    private final QueryFilter filter;

    public NotQueryFilter(final QueryFilter queryFilter) {
        filter = requireNonNull(queryFilter, "filter cannot be null");
    }

    @Override
    public String getFilters() {
        return "NOT " + filter.getFilters();
    }
}

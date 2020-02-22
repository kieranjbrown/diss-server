package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import io.vavr.collection.List;

import static java.util.Objects.requireNonNull;

public class QueryFilter {
    private List<Tuple3<String, QueryEquality, Object>> filters;

    public QueryFilter(final Tuple3<String, QueryEquality, Object>... filters) {
        this.filters = List.of(requireNonNull(filters, "filters cannot be null"));
    }

    public QueryFilter(final Tuple3<String, QueryEquality, Object> filter) {
        this.filters = List.of(requireNonNull(filter, "filter cannot be null"));
    }

    public QueryFilter(final List<Tuple3<String, QueryEquality, Object>> filters) {
        this.filters = requireNonNull(filters, "filters cannot be null");
    }

    public List<Tuple3<String, QueryEquality, Object>> getFilters() {
        return filters;
    }
}

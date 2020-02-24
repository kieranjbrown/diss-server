package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import io.vavr.collection.List;

import static java.util.Objects.requireNonNull;

//TODO: make interface, some contain single filter and some contain AND / OR ?
public class SingleQueryFilter implements QueryFilter {
    private List<Tuple3<String, QueryEquality, Object>> filters;

    public SingleQueryFilter(final Tuple3<String, QueryEquality, Object>... filters) {
        this.filters = List.of(requireNonNull(filters, "filters cannot be null"));
    }

    public SingleQueryFilter(final Tuple3<String, QueryEquality, Object> filter) {
        this.filters = List.of(requireNonNull(filter, "filter cannot be null"));
    }

    public SingleQueryFilter(final List<Tuple3<String, QueryEquality, Object>> filters) {
        this.filters = requireNonNull(filters, "filters cannot be null");
    }

    public String getFilters() {
        return "";
    }
}

package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import io.vavr.collection.List;

import static java.util.Objects.requireNonNull;

public class OrQueryFilter implements QueryFilter {
    private final List<QueryFilter> filterList;

    public OrQueryFilter(final Tuple3<String, QueryEquality, Object>... filters) {
        this.filterList = List.of(requireNonNull(filters, "filters cannot be null"))
                .map(SingleQueryFilter::new);
    }

    public OrQueryFilter(final List<QueryFilter> filters) {
        this.filterList = requireNonNull(filters, "filters cannot be null");
    }

    public String getFilters() {
        final List<String> filters = filterList.map(QueryFilter::getFilters);
        return "(" +
                filters.head() +
                " OR " +
                filters.tail().reduce((x, y) -> x + " AND " + y) +
                ")";
    }
}

package kieranbrown.bitemp.database;

import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.BitemporalModel;
import org.apache.commons.lang3.builder.EqualsBuilder;

import javax.persistence.Entity;

import static java.util.Objects.requireNonNull;

class SelectQuery<T extends BitemporalModel<T>> {

    private final QueryType queryType;
    private final Class<T> queryClass;
    private Map<String, Object> fields;
    private List<QueryFilter> filters;
    private int limit = -1;

    public SelectQuery(final QueryType queryType, final Class<T> clazz) {
        this.queryType = requireNonNull(queryType, "queryType cannot be null");
        queryClass = requireNonNull(clazz, "class cannot be null");
        fields = HashMap.empty();
        filters = List.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        SelectQuery selectQuery = (SelectQuery) o;

        return new EqualsBuilder()
                .append(queryType, selectQuery.queryType)
                .append(queryClass, selectQuery.queryClass)
                .append(fields, selectQuery.fields)
                .isEquals();
    }

    private String getFilters() {
        //TODO: tidy?
        return filters.length() > 0
                ? filters.init().foldLeft(" where", (x, y) -> x + " " + mapFilter(y) + " and") + " " +
                mapFilter(filters.last())
                : "";
    }

    private String mapFilter(final QueryFilter filter) {
        return filter.getFilters();
    }

    /*
     * -1 sets no limit
     */
    public SelectQuery<T> setLimit(final int limit) {
        this.limit = limit;
        return this;
    }

    public String build() {
        return "SELECT " +
                getFields() +
                " from " +
                getTableName() +
                getFilters() +
                getLimit();
    }

    private Object getFields() {
        return fields.length() > 0
                ? fields.keySet().reduce((x, y) -> x + ", " + y)
                : "*";
    }

    public SelectQuery<T> setFields(final Map<String, Object> fields) {
        this.fields = requireNonNull(fields, "fields cannot be null");
        return this;
    }

    private String getTableName() {
        return queryClass.getAnnotation(Entity.class).name();
    }

    public SelectQuery<T> setFilters(final List<QueryFilter> filters) {
        this.filters = filters;
        return this;
    }

    private String getLimit() {
        return limit == -1 ? "" : " limit " + limit;
    }
}

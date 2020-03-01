package kieranbrown.bitemp.database;

import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.BitemporalModel;
import kieranbrown.bitemp.utils.QueryUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

import javax.persistence.Entity;

import static io.vavr.API.*;
import static java.util.Objects.requireNonNull;
import static kieranbrown.bitemp.database.QueryType.*;

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
    public void setLimit(final int limit) {
        this.limit = limit;
    }

    public String build() {
        return Match(queryType).of(
                Case($(SELECT), o -> buildSelectQuery()),
                Case($(SELECT_DISTINCT), o -> buildDistinctSelectQuery()),
                Case($(INSERT), o -> buildInsertQuery())
        );
    }

    private String buildInsertQuery() {
        return "INSERT INTO " +
                getTableName() +
                " (" +
                getFields() +
                ") VALUES (" +
                getValues() +
                ")";
    }

    //TODO: figure out how to make this all prepared without a connection?
    private String buildSelectQuery() {
        return "SELECT " +
                getFields() +
                " from " +
                getTableName() +
                getFilters() +
                getLimit();
    }

    private String buildDistinctSelectQuery() {
        return "SELECT DISTINCT " +
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

    private String getValues() {
        return fields.values().map(QueryUtils::toString).reduce((x, y) -> x + ", " + y);
    }

    public void setFields(final Map<String, Object> fields) {
        this.fields = requireNonNull(fields, "fields cannot be null");
    }

    private String getTableName() {
        return queryClass.getAnnotation(Entity.class).name();
    }

    public void setFilters(final List<QueryFilter> filters) {
        this.filters = filters;
    }

    private String getLimit() {
        return limit == -1 ? "" : " limit " + limit;
    }
}

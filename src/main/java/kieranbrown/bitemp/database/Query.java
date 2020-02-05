package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.BitemporalModel;
import org.apache.commons.lang3.builder.EqualsBuilder;

import javax.persistence.Entity;

import static io.vavr.API.*;
import static java.util.Objects.requireNonNull;
import static kieranbrown.bitemp.database.QueryType.SELECT;
import static kieranbrown.bitemp.database.QueryType.SELECT_DISTINCT;

class Query<T extends BitemporalModel<T>> {

    private final QueryType queryType;
    private final Class<T> queryClass;
    private Map<String, Object> fields;
    private List<Tuple3<String, QueryEquality, Object>> filters;
    private int limit = -1;

    public Query(final QueryType queryType, final Class<T> clazz) {
        this.queryType = requireNonNull(queryType, "queryType cannot be null");
        queryClass = requireNonNull(clazz, "class cannot be null");
        fields = HashMap.empty();
        filters = List.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Query query = (Query) o;

        return new EqualsBuilder()
                .append(queryType, query.queryType)
                .append(queryClass, query.queryClass)
                .append(fields, query.fields)
                .isEquals();
    }

    public void setFilters(final List<Tuple3<String, QueryEquality, Object>> filters) {
        this.filters = filters;
    }

    /*
     * -1 sets no limit
     */
    public void setLimit(final int limit) {
        this.limit = limit;
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

    public String build() {
        return Match(queryType).of(
                Case($(x -> x.equals(SELECT) || x.equals(SELECT_DISTINCT)), buildSelectQuery())
        );
    }

    private Object getFields() {
        return fields.length() > 0
                ? fields.keySet().reduce((x, y) -> x + ", " + y)
                : "*";
    }

    public void setFields(final Map<String, Object> fields) {
        this.fields = requireNonNull(fields, "fields cannot be null");
    }

    private String getTableName() {
        return queryClass.getAnnotation(Entity.class).name();
    }

    private String getFilters() {
        //TODO: need to change dates to SQL friendly format
        return filters.length() > 0
                ? filters.foldLeft(" where", (x, y) -> x + " " + y._1 + " " + y._2.getValue() + " " + y._3)
                : "";
    }

    private String getLimit() {
        return limit == -1 ? "" : " limit " + limit;
    }
}

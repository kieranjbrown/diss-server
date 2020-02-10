package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.BitemporalModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

import javax.persistence.Entity;
import java.time.LocalDate;
import java.util.Date;

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

    public String build() {
        return Match(queryType).of(
                Case($(x -> x.equals(SELECT)), buildSelectQuery()),
                Case($(x -> x.equals(SELECT_DISTINCT)), buildDistinctSelectQuery())
        );
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

    public void setFields(final Map<String, Object> fields) {
        this.fields = requireNonNull(fields, "fields cannot be null");
    }

    private String getTableName() {
        return queryClass.getAnnotation(Entity.class).name();
    }

    private String getFilters() {
        return filters.length() > 0
                ? filters.init().foldLeft(" where ", (x, y) -> x + " " + y._1 + " " + y._2.getValue() + " " + toString(y._3) + " and ") +
                filters.last().apply((x, y, z) -> x + " " + y.getValue() + " " + toString(z))
                : "";
    }

    private String toString(final Object o) {
        if (o.getClass().equals(Date.class)) {
            final Date date = (Date) o;
            return String.format("'%s-%s-%s %s:%s:%s.000000'",
                    date.getYear(),
                    StringUtils.leftPad(String.valueOf(date.getMonth()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getDate()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getHours()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getMinutes()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getSeconds()), 2, "0"));
        } else if (o.getClass().equals(LocalDate.class)) {
            final LocalDate date = (LocalDate) o;
            return String.format("'%s-%s-%s'",
                    date.getYear(),
                    StringUtils.leftPad(String.valueOf(date.getMonthValue()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getDayOfYear()), 2, "0"));
        }
        return o.toString();
    }

    private String getLimit() {
        return limit == -1 ? "" : " limit " + limit;
    }
}

package kieranbrown.bitemp.database;

import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.BitemporalModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

import javax.persistence.Entity;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

import static io.vavr.API.*;
import static java.util.Objects.requireNonNull;
import static kieranbrown.bitemp.database.QueryType.*;

class Query<T extends BitemporalModel<T>> {

    private final QueryType queryType;
    private final Class<T> queryClass;
    private Map<String, Object> fields;
    private List<QueryFilter> filters;
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

    private String getFilters() {
        //TODO: tidy?
        return filters.length() > 0
                ? filters.init().foldLeft(" where", (x, y) -> x + " " + mapFilter(y) + " and") + " " +
                mapFilter(filters.last())
                : "";
    }

    private String mapFilter(final QueryFilter filter) {
//        return filter.getFilters().length() > 1
//                ? "(" +
//                filter.getFilters().map(x -> x._1 + " " + x._2.getValue() + " " + toString(x._3)).reduce((x, y) -> x + " OR " + y) +
//                ")"
//                : filter.getFilters().map(x -> x._1 + " " + x._2.getValue() + " " + toString(x._3)).reduce((x, y) -> x + " OR " + y);
        return "";
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
        return fields.values().map(this::toString).reduce((x, y) -> x + ", " + y);
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
                    StringUtils.leftPad(String.valueOf(date.getDayOfMonth()), 2, "0"));
        } else if (o.getClass().equals(String.class) || o.getClass().equals(Character.class) || o.getClass().equals(UUID.class)) {
            return String.format("'%s'", o);
        }
        return o.toString();
    }

    private String getLimit() {
        return limit == -1 ? "" : " limit " + limit;
    }
}

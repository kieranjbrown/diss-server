package kieranbrown.bitemp.database;

import io.vavr.collection.Map;
import kieranbrown.bitemp.models.BitemporalModel;
import org.apache.commons.lang3.builder.EqualsBuilder;

import javax.persistence.Entity;

import static io.vavr.API.*;
import static java.util.Objects.requireNonNull;
import static kieranbrown.bitemp.database.QueryType.SELECT;

class Query<T extends BitemporalModel<T>> {

    private final QueryType queryType;
    private final Class<T> queryClass;
    private Map<String, Object> fields;

    public Query(final QueryType queryType, final Class<T> clazz) {
        this.queryType = requireNonNull(queryType, "queryType cannot be null");
        queryClass = requireNonNull(clazz, "class cannot be null");
    }

    public QueryType getQueryType() {
        return queryType;
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

    public void setFields(final Map<String, Object> fields) {
        this.fields = requireNonNull(fields, "fields cannot be null");
    }

    public String build() {
        return Match(queryType).of(
                Case($(x -> x.equals(SELECT)), buildSelectQuery())
        );
    }

    //TODO: figure out how to make this all prepared without a connection?
    private String buildSelectQuery() {
        return "SELECT " +
                fields.keySet().reduce((x, y) -> x + ", " + y) +
                " from " +
                getTableName();
    }

    private String getTableName() {
        return queryClass.getAnnotation(Entity.class).name();
    }
}

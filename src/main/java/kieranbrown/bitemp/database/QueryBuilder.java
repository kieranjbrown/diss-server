package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import kieranbrown.bitemp.models.BitemporalModel;

import javax.persistence.Column;
import javax.persistence.EntityManager;
import java.lang.reflect.Field;
import java.util.Date;

import static java.util.Objects.requireNonNull;
import static kieranbrown.bitemp.database.QueryEquality.GREATER_THAN_EQUAL_TO;
import static kieranbrown.bitemp.database.QueryEquality.LESS_THAN_EQUAL_TO;

public class QueryBuilder<T extends BitemporalModel<T>> {
    private final Query<T> query;
    private final Class<T> queryClass;

    private final List<String> baseFields = List.of(
            "id",
            "version",
            "valid_time_start",
            "valid_time_end",
            "system_time_start",
            "system_time_end");

    private List<Tuple2<String, Object>> fields;
    private Option<List<T>> results;
    private List<Tuple3<String, QueryEquality, Object>> filters;

    private QueryBuilder(final QueryType queryType, final Class<T> clazz) {
        queryClass = clazz;
        this.query = new Query<>(queryType, clazz);
        results = Option.none();
        fields = List.empty();
        filters = List.empty();
    }

    public static <S extends BitemporalModel<S>> QueryBuilder<S> selectDistinct(final Class<S> clazz) {
        return new QueryBuilder<>(QueryType.SELECT_DISTINCT, clazz);
    }

    public static <S extends BitemporalModel<S>> QueryBuilder<S> select(final Class<S> clazz) {
        return new QueryBuilder<>(QueryType.SELECT, clazz);
    }

    //TODO: other types of queries

    //TODO: is there any point to this when Query can handle it
    //TODO: individual fields
    public QueryBuilder<T> allFields() {
        fields = Stream.of(queryClass.getDeclaredFields())
                .map(this::getName)
                .insertAll(0, baseFields)
                .map(x -> new Tuple2<>(x, null))
                .toList();
        return this;
    }

    public QueryBuilder<T> where(final String column, final QueryEquality equality, final Object value) {
        filters = filters.append(new Tuple3<>(column, equality, value));
        return this;
    }

    public QueryBuilder<T> betweenSystemTime(final Date startTime, final Date endTime) {
        filters = filters.appendAll(
                List.of(
                        new Tuple3<>("system_time_start", GREATER_THAN_EQUAL_TO, startTime),
                        new Tuple3<>("system_time_end", LESS_THAN_EQUAL_TO, endTime))
        );
        return this;
    }

    private String getName(final Field field) {
        final String annotationName = field.getAnnotation(Column.class).name();
        return "".equals(annotationName) ? field.getName() : annotationName;
    }

    public QueryBuilder<T> execute(final EntityManager entityManager) {
        requireNonNull(entityManager, "entityManager cannot be null");
        query.setFields(HashMap.ofEntries(fields));
        query.setFilters(filters);
        System.out.println(query.build());
        results = Option.of(List.ofAll(entityManager.createNativeQuery(query.build(), queryClass).getResultList()));
        return this;
    }

    public List<T> getResults() {
        return results.getOrElseThrow(
                () -> new IllegalStateException("call to getResults before executing query"));
        //TODO: method for single result?
    }
}

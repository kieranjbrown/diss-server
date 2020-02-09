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
import java.time.LocalDate;
import java.util.Date;

import static java.util.Objects.requireNonNull;
import static kieranbrown.bitemp.database.QueryEquality.*;

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

    /*
     * SYSTEM TIME METHODS
     * These methods are implemented in accordance with the SQL:2011 specification, discussed at
     * http://cs.unibo.it/~montesi/CBD/Articoli/Temporal%20features%20in%20SQL2011.pdf
     */

    public QueryBuilder<T> systemTimeBetween(final Date startTime, final Date endTime) {
        filters = filters.appendAll(
                List.of(
                        new Tuple3<>("system_time_start", GREATER_THAN_EQUAL_TO, startTime),
                        new Tuple3<>("system_time_end", LESS_THAN_EQUAL_TO, endTime))
        );
        return this;
    }

    public QueryBuilder<T> systemTimeAsOf(final Date time) {
        filters = filters.appendAll(
                List.of(
                        new Tuple3<>("system_time_start", LESS_THAN_EQUAL_TO, time),
                        new Tuple3<>("system_time_end", GREATER_THAN, time)
                )
        );
        return this;
    }

    public QueryBuilder<T> systemTimeFrom(final Date startTime, final Date endTime) {
        filters = filters.appendAll(
                List.of(
                        new Tuple3<>("system_time_start", GREATER_THAN_EQUAL_TO, startTime),
                        new Tuple3<>("system_time_end", LESS_THAN, endTime)
                )
        );
        return this;
    }

    /*
     * VALID TIME METHODS
     * These methods are implemented in accordance with the SQL:2011 specification, discussed at
     * http://cs.unibo.it/~montesi/CBD/Articoli/Temporal%20features%20in%20SQL2011.pdf and explained further at
     * https://docs.sqlstream.com/sql-reference-guide/temporal-predicates/ and
     * http://wwwlgis.informatik.uni-kl.de/cms/fileadmin/courses/SS2014/Neuere_Entwicklungen/Chapter_10_-_Temporal_DM.pdf
     * */

    //CONTAINS x
    public QueryBuilder<T> validTimeContains(final LocalDate startDate, final LocalDate endDate) {
        filters = filters.appendAll(
                List.of(
                        new Tuple3<>("valid_time_start", GREATER_THAN_EQUAL_TO, startDate),
                        new Tuple3<>("valid_time_end", LESS_THAN_EQUAL_TO, endDate)
                )
        );
        return this;
    }

    //x EQUALS y
    public QueryBuilder<T> validTimeEquals(final LocalDate startDate, final LocalDate endDate) {
        filters = filters.appendAll(
                List.of(
                        new Tuple3<>("valid_time_start", EQUALS, startDate),
                        new Tuple3<>("valid_time_end", EQUALS, endDate)
                )
        );
        return this;
    }

    //x PRECEDES Y
    public QueryBuilder<T> validTimePrecedes(final LocalDate startDate, final LocalDate endDate) {
        filters = filters.appendAll(
                List.of(
                        new Tuple3<>("valid_time_start", LESS_THAN, startDate),
                        new Tuple3<>("valid_time_end", LESS_THAN_EQUAL_TO, endDate)
                )
        );
        return this;
    }

    //x IMMEDIATELY PRECEDES y
    public QueryBuilder<T> validTimeImmediatelyPrecedes(final LocalDate startDate, final LocalDate endDate) {
        filters = filters.appendAll(
                List.of(
                        new Tuple3<>("valid_time_start", LESS_THAN, startDate),
                        new Tuple3<>("valid_time_end", EQUALS, endDate)
                )
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
        results = Option.of(List.ofAll(entityManager.createNativeQuery(query.build(), queryClass).getResultList()));
        return this;
    }

    public List<T> getResults() {
        return results.getOrElseThrow(
                () -> new IllegalStateException("call to getResults before executing query"));
        //TODO: method for single result?
    }
}

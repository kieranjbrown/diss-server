package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.LinkedHashMap;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import kieranbrown.bitemp.models.BitemporalModel;

import javax.persistence.Column;
import javax.persistence.EntityManager;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Date;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static kieranbrown.bitemp.database.QueryEquality.*;

public class QueryBuilder<T extends BitemporalModel<T>> {
    private final Query<T> query;
    private final Class<T> queryClass;
    private final QueryType queryType;

    private final List<String> baseFields = List.of(
            "id",
            "version",
            "valid_time_start",
            "valid_time_end",
            "system_time_start",
            "system_time_end");

    private List<Tuple2<String, Object>> fields;
    private Option<List<T>> results;
    private List<QueryFilter> filters;

    private QueryBuilder(final QueryType queryType, final Class<T> clazz) {
        queryClass = clazz;
        this.queryType = queryType;
        this.query = new Query<>(queryType, clazz);
        results = Option.none();
        fields = baseFields.map(x -> new Tuple2<>(x, null));
        filters = List.empty();
    }

    public static <S extends BitemporalModel<S>> QueryBuilder<S> selectDistinct(final Class<S> clazz) {
        return new QueryBuilder<>(QueryType.SELECT_DISTINCT, clazz);
    }

    public static <S extends BitemporalModel<S>> QueryBuilder<S> select(final Class<S> clazz) {
        return new QueryBuilder<>(QueryType.SELECT, clazz);
    }

    //TODO: split these out into other classes e.g. InsertQueryBuilder etc?
    public static <S extends BitemporalModel<S>> QueryBuilder<S> insert(final Class<S> clazz) {
        return new QueryBuilder<>(QueryType.INSERT, clazz);
    }

    public QueryBuilder<T> from(final T object) {
        fields = List.of(
                new Tuple2<>("id", object.getTradeKey().getId()),
                new Tuple2<>("version", object.getTradeKey().getVersion()),
                new Tuple2<>("valid_time_start", object.getValidTimeStart()),
                new Tuple2<>("valid_time_end", object.getValidTimeEnd()),
                new Tuple2<>("system_time_start", object.getSystemTimeStart()),
                new Tuple2<>("system_time_end", object.getSystemTimeEnd()));

        fields = fields.appendAll(Stream.of(queryClass.getDeclaredFields())
                .map(x -> new Tuple2<>(x, getFieldName(x)))
                .map(x -> new Tuple2<>(getColumnName(x._1), getFieldValue(x._2, object)))
                .collect(Collectors.toList()));
        return this;
    }

    private String getFieldName(final Field field) {
        return field.getName();
    }

    private Object getFieldValue(final String fieldName, final T object) {
        //TODO: tidy this mess
        try {
            final Field declaredField = queryClass.getDeclaredField(fieldName);
            declaredField.setAccessible(true);
            return declaredField.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("error");
    }

    //TODO: other types of queries

    //TODO: remove because can only retrieve all the fields
    public QueryBuilder<T> allFields() {
        fields = Stream.of(queryClass.getDeclaredFields())
                .map(this::getColumnName)
                .insertAll(0, baseFields)
                .map(x -> new Tuple2<>(x, null))
                .toList();
        return this;
    }

    private String getColumnName(final Field field) {
        final String annotationName = field.getAnnotation(Column.class).name();
        return "".equals(annotationName) ? field.getName() : annotationName;
    }

    public QueryBuilder<T> addField(final String field) {
        fields = fields.append(new Tuple2<>(field, null));
        return this;
    }

    public QueryBuilder<T> where(final String column, final QueryEquality equality, final Object value) {
        filters = filters.append(new SingleQueryFilter(new Tuple3<>(column, equality, value)));
        return this;
    }

    //TODO: make queryFilter class to allow for OR queries, maybe make internals of this class use it it too?
//    public QueryBuilder<T> where (final QueryFilter queryFilter) {
//        return this;
//    }

    /*
     * SYSTEM TIME METHODS
     * These methods are implemented in accordance with the SQL:2011 specification, discussed at
     * http://cs.unibo.it/~montesi/CBD/Articoli/Temporal%20features%20in%20SQL2011.pdf
     */

    public QueryBuilder<T> systemTimeBetween(final Date startTime, final Date endTime) {
        filters = filters.appendAll(
                List.of(
                        new SingleQueryFilter(new Tuple3<>("system_time_start", GREATER_THAN_EQUAL_TO, startTime)),
                        new SingleQueryFilter(new Tuple3<>("system_time_end", LESS_THAN_EQUAL_TO, endTime))
                )
        );
        return this;
    }

    public QueryBuilder<T> systemTimeAsOf(final Date time) {
        filters = filters.appendAll(
                List.of(
                        new SingleQueryFilter(new Tuple3<>("system_time_start", LESS_THAN_EQUAL_TO, time)),
                        new SingleQueryFilter(new Tuple3<>("system_time_end", GREATER_THAN, time))
                )
        );
        return this;
    }

    public QueryBuilder<T> systemTimeFrom(final Date startTime, final Date endTime) {
        filters = filters.appendAll(
                List.of(
                        new SingleQueryFilter(new Tuple3<>("system_time_start", GREATER_THAN_EQUAL_TO, startTime)),
                        new SingleQueryFilter(new Tuple3<>("system_time_end", LESS_THAN, endTime))
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
                        new SingleQueryFilter(new Tuple3<>("valid_time_start", GREATER_THAN_EQUAL_TO, startDate)),
                        new SingleQueryFilter(new Tuple3<>("valid_time_end", LESS_THAN_EQUAL_TO, endDate))
                )
        );
        return this;
    }

    //x EQUALS y
    public QueryBuilder<T> validTimeEquals(final LocalDate startDate, final LocalDate endDate) {
        filters = filters.appendAll(
                List.of(
                        new SingleQueryFilter(new Tuple3<>("valid_time_start", EQUALS, startDate)),
                        new SingleQueryFilter(new Tuple3<>("valid_time_end", EQUALS, endDate))
                )
        );
        return this;
    }

    //x PRECEDES Y
    public QueryBuilder<T> validTimePrecedes(final LocalDate startDate) {
        //TODO: Should be doing checks here to enforce startDate <= endDate?
        filters = filters.append(new SingleQueryFilter(new Tuple3<>("valid_time_end", LESS_THAN_EQUAL_TO, startDate)));
        return this;
    }

    //x IMMEDIATELY PRECEDES y
    public QueryBuilder<T> validTimeImmediatelyPrecedes(final LocalDate startDate) {
        filters = filters.append(new SingleQueryFilter(new Tuple3<>("valid_time_end", EQUALS, startDate)));
        return this;
    }

    //x SUCCEEDS y
    public QueryBuilder<T> validTimeSucceeds(final LocalDate endDate) {
        filters = filters.append(new SingleQueryFilter(new Tuple3<>("valid_time_start", GREATER_THAN_EQUAL_TO, endDate)));
        return this;
    }

    //x IMMEDIATELY SUCCEEDS y
    public QueryBuilder<T> validTimeImmediatelySucceeds(final LocalDate endDate) {
        filters = filters.append(new SingleQueryFilter(new Tuple3<>("valid_time_start", EQUALS, endDate)));
        return this;
    }

    //x OVERLAPS y
    public QueryBuilder<T> validTimeOverlaps(final LocalDate startDate, final LocalDate endDate) {
        filters = filters.append(
                new OrQueryFilter(
                        new AndQueryFilter(
                                new SingleQueryFilter("valid_time_start", GREATER_THAN_EQUAL_TO, startDate),
                                new SingleQueryFilter("valid_time_end", LESS_THAN_EQUAL_TO, endDate)
                        ),
                        new AndQueryFilter(
                                new SingleQueryFilter("valid_time_start", GREATER_THAN_EQUAL_TO, startDate),
                                new SingleQueryFilter("valid_time_start", LESS_THAN, endDate),
                                new SingleQueryFilter("valid_time_end", GREATER_THAN, endDate)
                        ),
                        new AndQueryFilter(
                                new SingleQueryFilter("valid_time_start", LESS_THAN_EQUAL_TO, startDate),
                                new SingleQueryFilter("valid_time_end", GREATER_THAN_EQUAL_TO, endDate)
                        ),
                        new AndQueryFilter(
                                new SingleQueryFilter("valid_time_start", LESS_THAN_EQUAL_TO, startDate),
                                new SingleQueryFilter("valid_time_end", GREATER_THAN_EQUAL_TO, startDate),
                                new SingleQueryFilter("valid_time_start", LESS_THAN, endDate)
                        )
                )
        );
        return this;
    }

    public QueryBuilder<T> execute(final EntityManager entityManager) {
        requireNonNull(entityManager, "entityManager cannot be null");
        query.setFields(LinkedHashMap.ofEntries(fields));
        query.setFilters(filters);
        //TODO: needs changing for queries that don't return results
        //TODO: change to result mapper so can actually choose which fields you want?
        if (queryType.equals(QueryType.SELECT_DISTINCT) || queryType.equals(QueryType.SELECT)) {
            System.out.println(query.build());
            results = Option.of(List.ofAll(entityManager.createNativeQuery(query.build(), queryClass).getResultList()));
        } else {
            entityManager.createNativeQuery(query.build()).executeUpdate();
//            results = Option.of
        }
        return this;
    }

    public List<T> getResults() {
        return results.getOrElseThrow(
                () -> new IllegalStateException("call to getResults before executing query"));
        //TODO: method for single result?
    }
}

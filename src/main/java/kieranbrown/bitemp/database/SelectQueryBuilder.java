package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import kieranbrown.bitemp.models.BitemporalModel;

import javax.persistence.EntityManager;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;
import static kieranbrown.bitemp.database.QueryEquality.*;

public class SelectQueryBuilder<T extends BitemporalModel<T>> {
    private final SelectQuery<T> query;
    private final Class<T> queryClass;

    private Option<List<T>> results;
    private List<QueryFilter> filters;

    //TODO: should all be made into BiFunctions like this and extracted elsewhere?
    public static final BiFunction<LocalDate, LocalDate, QueryFilter> validTimeOverlaps = (startDate, endDate) -> new OrQueryFilter(
            new AndQueryFilter(
                    new SingleQueryFilter("valid_time_start", GREATER_THAN, startDate),
                    new NotQueryFilter(
                            new AndQueryFilter(
                                    new SingleQueryFilter("valid_time_start", GREATER_THAN_EQUAL_TO, endDate),
                                    new SingleQueryFilter("valid_time_end", GREATER_THAN_EQUAL_TO, endDate)
                            )
                    )
            ),
            new AndQueryFilter(
                    new SingleQueryFilter("valid_time_start", LESS_THAN, startDate),
                    new NotQueryFilter(
                            new AndQueryFilter(
                                    new SingleQueryFilter("valid_time_end", LESS_THAN_EQUAL_TO, startDate),
                                    new SingleQueryFilter("valid_time_end", LESS_THAN_EQUAL_TO, endDate)
                            )
                    )
            ),
            new AndQueryFilter(
                    new SingleQueryFilter("valid_time_start", EQUALS, startDate),
                    new OrQueryFilter(
                            new SingleQueryFilter("valid_time_end", EQUALS, endDate),
                            new SingleQueryFilter("valid_time_end", DOES_NOT_EQUAL, endDate)
                    )
            )
    );

    //TODO: is queryType needed anymore?
    SelectQueryBuilder(final Class<T> clazz) {
        queryClass = clazz;
        this.query = new SelectQuery<>(clazz);
        results = Option.none();
        filters = List.empty();
    }

    public SelectQueryBuilder<T> where(final String column, final QueryEquality equality, final Object value) {
        filters = filters.append(new SingleQueryFilter(new Tuple3<>(column, equality, value)));
        return this;
    }

    public SelectQueryBuilder<T> where(final QueryFilter queryFilter) {
        filters = filters.append(queryFilter);
        return this;
    }

    public SelectQueryBuilder<T> where(final QueryFilter... queryFilters) {
        filters = filters.appendAll(List.of(queryFilters));
        return this;
    }

    public SelectQueryBuilder<T> where(final List<QueryFilter> queryFilters) {
        filters = filters.appendAll(queryFilters);
        return this;
    }

    public SelectQueryBuilder<T> where(final Stream<QueryFilter> queryFilters) {
        filters = filters.appendAll(queryFilters);
        return this;
    }

    /*
     * SYSTEM TIME METHODS
     * These methods are implemented in accordance with the SQL:2011 specification, discussed at
     * http://cs.unibo.it/~montesi/CBD/Articoli/Temporal%20features%20in%20SQL2011.pdf
     */

    public SelectQueryBuilder<T> systemTimeBetween(final LocalDateTime startTime, final LocalDateTime endTime) {
        filters = filters.appendAll(
                List.of(
                        new SingleQueryFilter(new Tuple3<>("system_time_start", GREATER_THAN_EQUAL_TO, startTime)),
                        new SingleQueryFilter(new Tuple3<>("system_time_end", LESS_THAN_EQUAL_TO, endTime))
                )
        );
        return this;
    }

    public SelectQueryBuilder<T> systemTimeAsOf(final LocalDateTime time) {
        filters = filters.appendAll(
                List.of(
                        new SingleQueryFilter(new Tuple3<>("system_time_start", LESS_THAN_EQUAL_TO, time)),
                        new SingleQueryFilter(new Tuple3<>("system_time_end", GREATER_THAN, time))
                )
        );
        return this;
    }

    public SelectQueryBuilder<T> systemTimeFrom(final LocalDateTime startTime, final LocalDateTime endTime) {
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
    public SelectQueryBuilder<T> validTimeContains(final LocalDate startDate, final LocalDate endDate) {
        filters = filters.appendAll(
                List.of(
                        new SingleQueryFilter(new Tuple3<>("valid_time_start", GREATER_THAN_EQUAL_TO, startDate)),
                        new SingleQueryFilter(new Tuple3<>("valid_time_end", LESS_THAN_EQUAL_TO, endDate))
                )
        );
        return this;
    }

    //x EQUALS y
    public SelectQueryBuilder<T> validTimeEquals(final LocalDate startDate, final LocalDate endDate) {
        filters = filters.appendAll(
                List.of(
                        new SingleQueryFilter(new Tuple3<>("valid_time_start", EQUALS, startDate)),
                        new SingleQueryFilter(new Tuple3<>("valid_time_end", EQUALS, endDate))
                )
        );
        return this;
    }

    //x PRECEDES Y
    public SelectQueryBuilder<T> validTimePrecedes(final LocalDate startDate) {
        //TODO: Should be doing checks here to enforce startDate <= endDate?
        filters = filters.append(new SingleQueryFilter(new Tuple3<>("valid_time_end", LESS_THAN_EQUAL_TO, startDate)));
        return this;
    }

    //x IMMEDIATELY PRECEDES y
    public SelectQueryBuilder<T> validTimeImmediatelyPrecedes(final LocalDate startDate) {
        filters = filters.append(new SingleQueryFilter(new Tuple3<>("valid_time_end", EQUALS, startDate)));
        return this;
    }

    //x SUCCEEDS y
    public SelectQueryBuilder<T> validTimeSucceeds(final LocalDate endDate) {
        filters = filters.append(new SingleQueryFilter(new Tuple3<>("valid_time_start", GREATER_THAN_EQUAL_TO, endDate)));
        return this;
    }

    //x IMMEDIATELY SUCCEEDS y
    public SelectQueryBuilder<T> validTimeImmediatelySucceeds(final LocalDate endDate) {
        filters = filters.append(new SingleQueryFilter(new Tuple3<>("valid_time_start", EQUALS, endDate)));
        return this;
    }

    //x OVERLAPS y
    //Logic for this method was refined from this website, as it was originally implemented incorrectly
    //https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/3VIgdwHNVU~tsnNiIR1aEw
    public SelectQueryBuilder<T> validTimeOverlaps(final LocalDate startDate, final LocalDate endDate) {
        filters = filters.append(validTimeOverlaps.apply(startDate, endDate));
        return this;
    }

    @SuppressWarnings("unchecked")
    public SelectQueryBuilder<T> execute(final EntityManager entityManager) {
        requireNonNull(entityManager, "entityManager cannot be null");
        query.setFilters(filters);
        results = Option.of(List.ofAll(entityManager.createNativeQuery(query.build(), queryClass).getResultList()));
        return this;
    }

    public List<T> getResults() {
        return results.getOrElseThrow(
                () -> new IllegalStateException("call to getResults before executing query"));
    }
}

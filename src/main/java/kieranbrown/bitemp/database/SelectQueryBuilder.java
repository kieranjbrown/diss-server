package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.List;
import io.vavr.control.Option;
import kieranbrown.bitemp.models.BitemporalModel;

import javax.persistence.EntityManager;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static java.util.Objects.requireNonNull;
import static kieranbrown.bitemp.database.QueryEquality.*;

public class SelectQueryBuilder<T extends BitemporalModel<T>> {
    private final SelectQuery<T> selectQuery;
    private final Class<T> queryClass;

    private List<Tuple2<String, Object>> fields;
    private Option<List<T>> results;
    private List<QueryFilter> filters;

    //TODO: is queryType needed anymore?
    SelectQueryBuilder(final QueryType queryType, final Class<T> clazz) {
        queryClass = clazz;
        this.selectQuery = new SelectQuery<>(queryType, clazz);
        results = Option.none();
        fields = List.of(
                "id",
                "valid_time_start",
                "valid_time_end",
                "system_time_start",
                "system_time_end")
                .map(x -> new Tuple2<>(x, null));
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
    public SelectQueryBuilder<T> validTimeOverlaps(final LocalDate startDate, final LocalDate endDate) {
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

    //TODO: change to DataSource / JdbcTemplate? could be autowired in the Factory so user doesn't have to pass it
    @SuppressWarnings("unchecked")
    public SelectQueryBuilder<T> execute(final EntityManager entityManager) {
        requireNonNull(entityManager, "entityManager cannot be null");
        selectQuery.setFilters(filters);
        System.out.println(selectQuery.build());
        results = Option.of(List.ofAll(entityManager.createNativeQuery(selectQuery.build(), queryClass).getResultList()));
        return this;
    }

    public List<T> getResults() {
        return results.getOrElseThrow(
                () -> new IllegalStateException("call to getResults before executing query"));
    }
}

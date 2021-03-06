package kieranbrown.bitemp.database;

import com.rits.cloning.Cloner;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.control.Option;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.BitemporalModel;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.EntityManagerFactoryInfo;

import javax.persistence.EntityManager;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.util.ArrayList;

import static java.util.Objects.requireNonNull;

public class UpdateQueryBuilder<T extends BitemporalModel<T>> {
    private final Class<T> queryClass;
    private List<Tuple2<String, Object>> fields;
    private List<QueryFilter> filters;
    private Option<Tuple2<LocalDate, LocalDate>> validTimePeriod;

    public UpdateQueryBuilder(final Class<T> clazz) {
        queryClass = requireNonNull(clazz, "class cannot be null");
        this.fields = List.empty();
        this.filters = List.empty();
        this.validTimePeriod = Option.none();
    }

    public UpdateQueryBuilder<T> forValidTimePeriod(final LocalDate startDate, final LocalDate endDate) {
        this.validTimePeriod = Option.of(new Tuple2<>(startDate, endDate));
        return this;
    }

    public UpdateQueryBuilder<T> set(final String column, final Object value) {
        this.fields = this.fields.append(new Tuple2<>(column, value));
        return this;
    }

    public UpdateQueryBuilder<T> where(final QueryFilter queryFilter) {
        this.filters = this.filters.append(queryFilter);
        return this;
    }

    public void execute(final EntityManager entityManager) throws InvalidPeriodException {
        final DataSource dataSource = getDataSource(entityManager);
        validTimePeriod.peek(validTime -> updateValidTimePeriod(validTime, entityManager, dataSource))
                .onEmpty(() -> update(dataSource));

//        final List<T> results = new SelectQueryBuilder<>(queryClass).where(filters)
//                .execute(entityManager)
//                .getResults();
//
//        results.forEach(entityManager::detach);
//
//        final LocalDateTime now = LocalDateTime.now();
//        final String updateQuery = new UpdateQuery<>(queryClass)
//                .addFields(fields.append(new Tuple2<>("system_time_start", now)))
//                .addFilters(List.of(new OrQueryFilter(results
//                        .map(x -> new AndQueryFilter(
//                                new SingleQueryFilter("id", QueryEquality.EQUALS, x.getBitemporalKey().getId()),
//                                new SingleQueryFilter("system_time_start", QueryEquality.EQUALS, x.getSystemTimeStart()),
//                                new SingleQueryFilter("system_time_end", QueryEquality.EQUALS, x.getSystemTimeEnd())
//                        )))
//                )).build();
//
//        new JdbcTemplate(dataSource).execute(updateQuery);
//
//        new InsertQueryBuilder<>(queryClass)
//                .fromAll(results.map(x -> x.setSystemTimeEnd(now)))
//                .execute(entityManager);
    }

    private void updateValidTimePeriod(final Tuple2<LocalDate, LocalDate> validTime, final EntityManager entityManager, final DataSource dataSource) {
        final SelectQueryBuilder<T> selectQueryBuilder = new SelectQueryBuilder<>(queryClass).where(filters);
        final List<T> noTimeResults = selectQueryBuilder.execute(entityManager).getResults();
        final List<T> timeResults = selectQueryBuilder.validTimeOverlaps(validTime._1, validTime._2).execute(entityManager).getResults();

        if (timeResults.length() > 0) {
            timeResults.forEach(entityManager::detach);

            final String firstSql = new UpdateQuery<>(queryClass)
                    .addFields(List.of(new Tuple2<>("valid_time_end", validTime._1)))
                    .addFilters(filters.append(
                            new OrQueryFilter(timeResults.map(y -> new SingleQueryFilter("id", QueryEquality.EQUALS, y.getBitemporalKey().getId())))
                    ))
                    .build();
            new JdbcTemplate(dataSource).execute(firstSql);

            final java.util.List<T> toUpdate = new ArrayList<>();
            final java.util.List<T> toInsert = new ArrayList<>();

            timeResults.forEach(result -> {
                final BitemporalKey key = result.getBitemporalKey();

                toUpdate.add(new Cloner().deepClone(result).setBitemporalKey(new BitemporalKey.Builder()
                        .setTradeId(key.getId())
                        .setValidTimeStart(validTime._1)
                        .setValidTimeEnd(validTime._2)
                        .build()
                ));

                toInsert.add(new Cloner().deepClone(result).setBitemporalKey(new BitemporalKey.Builder()
                        .setTradeId(key.getId())
                        .setValidTimeStart(validTime._2)
                        .setValidTimeEnd(key.getValidTimeEnd())
                        .build()));
            });

            try {
                new InsertQueryBuilder<>(queryClass)
                        .fromAll(List.ofAll(toUpdate))
                        .execute(entityManager);

                new JdbcTemplate(dataSource).execute(new UpdateQuery<>(queryClass)
                        .addFields(fields)
                        .addFilters(filters.append(new SingleQueryFilter("valid_time_start", QueryEquality.EQUALS, validTime._1)))
                        .build()
                );

                new InsertQueryBuilder<>(queryClass)
                        .fromAll(List.ofAll(toInsert))
                        .execute(entityManager);
            } catch (final InvalidPeriodException e) {
            }
        }
        if (noTimeResults.removeAll(timeResults).length() > 0) {
            final String updateOthersSql = new UpdateQuery<>(queryClass)
                    .addFields(fields)
                    .addFilters(filters.append(
                            new NotQueryFilter(new OrQueryFilter(timeResults.map(y -> new SingleQueryFilter("id", QueryEquality.EQUALS, y.getBitemporalKey().getId()))))
                    ))
                    .build();
            new JdbcTemplate(dataSource).execute(updateOthersSql);
        }
    }

    private void update(final DataSource dataSource) {
        final String sql = new UpdateQuery<>(queryClass)
                .addFields(fields)
                .addFilters(filters)
                .build();

//        System.out.println(sql);
        new JdbcTemplate(dataSource).execute(sql);
    }

    private DataSource getDataSource(final EntityManager entityManager) {
        return ((EntityManagerFactoryInfo) entityManager.getEntityManagerFactory()).getDataSource();
    }
}

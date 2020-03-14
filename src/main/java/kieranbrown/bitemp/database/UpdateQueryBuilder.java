package kieranbrown.bitemp.database;

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

    public void execute(final EntityManager entityManager) {
        final DataSource dataSource = getDataSource(entityManager);
        validTimePeriod.peek(x -> {
            final SelectQueryBuilder<T> selectQueryBuilder = new SelectQueryBuilder<>(QueryType.SELECT, queryClass);
            filters.forEach(selectQueryBuilder::where);
            final List<T> noTimeResults = selectQueryBuilder.execute(entityManager).getResults();
            final List<T> timeResults = selectQueryBuilder.validTimeOverlaps(x._1, x._2).execute(entityManager).getResults();

            if (timeResults.length() > 0) {
                timeResults.forEach(entityManager::detach);

                final String firstSql = new UpdateQuery<>(queryClass)
                        .addFields(fields.append(new Tuple2<>("valid_time_end", x._1)))
                        .addFilters(filters.append(
                                new OrQueryFilter(timeResults.map(y -> new SingleQueryFilter("id", QueryEquality.EQUALS, y.getBitemporalKey().getId())))
                        ))
                        .build();
                System.out.println("firstSql:" + firstSql);
                new JdbcTemplate(dataSource).execute(firstSql);
                //TODO: maybe figure out converting these to batch updates in a single query?
                timeResults.forEach(result -> {
                    final BitemporalKey key = result.getBitemporalKey();

                    try {
                        //this needs the updates applied to it
                        final T t = result.setBitemporalKey(new BitemporalKey.Builder()
                                .setTradeId(key.getId())
                                .setValidTimeStart(x._1)
                                .setValidTimeEnd(x._2)
                                .build()
                        );

                        new InsertQueryBuilder<>(queryClass)
                                .from(t)
                                .execute(entityManager);

                        new JdbcTemplate(dataSource).execute(new UpdateQuery<>(queryClass)
                                .addFields(fields)
                                .addFilters(filters.append(new SingleQueryFilter("valid_time_start", QueryEquality.EQUALS, x._1)))
                                .build()
                        );

                        final T finalRow = result.setBitemporalKey(new BitemporalKey.Builder()
                                .setTradeId(key.getId())
                                .setValidTimeStart(x._2)
                                .setValidTimeEnd(key.getValidTimeEnd())
                                .build());

                        new InsertQueryBuilder<>(queryClass)
                                .from(finalRow)
                                .execute(entityManager);
                    } catch (final OverlappingKeyException e) {
//                    throw e;
                    }
                });
            }
            if (noTimeResults.removeAll(timeResults).length() > 0) {
                final String updateOthersSql = new UpdateQuery<>(queryClass)
                        .addFields(fields)
                        .addFilters(filters.append(
                                new NotQueryFilter(new OrQueryFilter(timeResults.map(y -> new SingleQueryFilter("id", QueryEquality.EQUALS, y.getBitemporalKey().getId()))))
                        ))
                        .build();
                System.out.println("updateOtherSql:" + updateOthersSql);
                new JdbcTemplate(dataSource).execute(updateOthersSql);
            }
        }).onEmpty(() -> update(dataSource));
    }

    private void update(final DataSource dataSource) {
        final String sql = new UpdateQuery<>(queryClass)
                .addFields(fields)
                .addFilters(filters)
                .build();

        System.out.println(sql);
        new JdbcTemplate(dataSource).execute(sql);
    }

    private DataSource getDataSource(EntityManager entityManager) {
        return ((EntityManagerFactoryInfo) entityManager.getEntityManagerFactory()).getDataSource();
    }
}

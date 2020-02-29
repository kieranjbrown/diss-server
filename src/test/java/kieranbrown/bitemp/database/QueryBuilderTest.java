package kieranbrown.bitemp.database;

import com.google.common.collect.ImmutableList;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

import static kieranbrown.bitemp.database.QueryBuilder.select;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryBuilderTest {

    @SpringJUnitConfig
    @DataJpaTest
    @Nested
    class SelectQueries {
        private final BitemporalKey KEY = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
        @Autowired
        private TradeWriteRepository repository;
        @PersistenceContext
        private EntityManager entityManager;

        @Test
        void canCreateQueryForDistinctResult() {
            assertThat(QueryBuilder.selectDistinct(Trade.class)).isNotNull()
                    .hasFieldOrPropertyWithValue("query", new Query<>(QueryType.SELECT_DISTINCT, Trade.class))
                    .hasFieldOrPropertyWithValue("queryClass", Trade.class);
        }

        @Test
        void canCreateQueryForMultipleResults() {
            assertThat(QueryBuilder.select(Trade.class)).isNotNull()
                    .hasFieldOrPropertyWithValue("query", new Query<>(QueryType.SELECT, Trade.class))
                    .hasFieldOrPropertyWithValue("queryClass", Trade.class);
        }

        @Test
        void canEvaluateSingleSelectQueryAndReturnResult() {
            final Trade trade = new Trade().setTradeKey(KEY)
                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                    .setSystemTimeStart(new Date(2020, 1, 20, 3, 45, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 45, 0))
                    .setVolume(200)
                    .setPrice(new BigDecimal("123.45"))
                    .setMarketLimitFlag('M')
                    .setBuySellFlag('B')
                    .setStock("GOOGL");

            repository.save(trade);
            assertThat(
                    QueryBuilder.selectDistinct(Trade.class)
                            .execute(entityManager)
                            .getResults())
                    .isNotNull()
                    .isNotEmpty()
                    .first()
                    .usingRecursiveComparison()
                    .isEqualTo(trade);
        }

        @Test
        void canAddSingleField() {
            final Trade trade = new Trade().setTradeKey(KEY)
                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                    .setSystemTimeStart(new Date(2020, 1, 20, 3, 45, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 45, 0))
                    .setVolume(200)
                    .setPrice(new BigDecimal("123.45"))
                    .setMarketLimitFlag('M')
                    .setBuySellFlag('B')
                    .setStock("GOOGL");

            repository.save(trade);

            final QueryBuilder<Trade> queryBuilder = QueryBuilder.select(Trade.class).addField("stock").execute(entityManager);
            final List<Trade> results = queryBuilder.getResults();
            assertThat(results).isNotNull().hasSize(1);
            assertThat(queryBuilder).extracting("query")
                    .hasFieldOrPropertyWithValue("fields", HashMap.ofEntries(
                            new Tuple2<>("id", null),
                            new Tuple2<>("version", null),
                            new Tuple2<>("valid_time_start", null),
                            new Tuple2<>("valid_time_end", null),
                            new Tuple2<>("system_time_start", null),
                            new Tuple2<>("system_time_end", null),
                            new Tuple2<>("stock", null)
                    ));
        }

        @Test
        void throwsForNullEntityManager() {
            assertThat(assertThrows(NullPointerException.class, () -> select(Trade.class).execute(null)))
                    .hasMessage("entityManager cannot be null");
        }

        @Test
        void throwsIfResultsAreRetrievedBeforeCodeIsExecuted() {
            assertThat(assertThrows(IllegalStateException.class, () -> select(Trade.class).getResults()))
                    .hasMessage("call to getResults before executing query");
        }

        @Test
        void settingFilterAffectsQuery() {
            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(2020, 1, 20, 3, 45, 0))
                            .setSystemTimeEnd(new Date(2020, 1, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(2020, 1, 20, 3, 45, 0))
                            .setSystemTimeEnd(new Date(2020, 1, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            final QueryBuilder<Trade> query = select(Trade.class);
            query.where("version", QueryEquality.EQUALS, 3);
            query.execute(entityManager);
            final List<Trade> results = query.getResults();

            assertThat(results).isNotNull().hasSize(1);
        }

        @Test
        void systemTimeBetweenFilterAffectsResults() {
            //TODO: convert all system dates to ZonedDateTime or something similar: Hibernate takes literal values e.g. 1900+2020,
            //whereas val.getYear() returns only 2020 so big disparity
            final Date startRange = new Date(2020, 1, 10, 0, 0, 0);
            final Date endRange = new Date(2020, 1, 20, 0, 0, 0);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 12, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 20, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            final QueryBuilder<Trade> queryBuilder = QueryBuilder.select(Trade.class);
            final List<Trade> results = queryBuilder
                    .systemTimeBetween(startRange, endRange)
                    .execute(entityManager)
                    .getResults();

            assertThat(results).isNotNull().hasSize(3);
            results.forEach(x -> {
                assertThat(x.getSystemTimeStart()).isAfterOrEqualTo("2020-01-10 00:00:00.000000");
                assertThat(x.getSystemTimeEnd()).isBeforeOrEqualTo("2020-01-20 00:00:00.000000");
            });
        }

        @Test
        void systemTimeFromFilterAffectsResults() {
            final Date startRange = new Date(2020, 1, 10, 0, 0, 0);
            final Date endRange = new Date(2020, 1, 20, 0, 0, 0);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 12, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 20, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            final QueryBuilder<Trade> queryBuilder = QueryBuilder.select(Trade.class);
            final List<Trade> results = queryBuilder
                    .systemTimeFrom(startRange, endRange)
                    .execute(entityManager)
                    .getResults();

            assertThat(results).isNotNull().hasSize(2);
            results.forEach(x -> {
                assertThat(x.getSystemTimeStart()).isAfterOrEqualTo("2020-01-10 00:00:00.000000");
                assertThat(x.getSystemTimeEnd()).isBefore("2020-01-20 00:00:00.000000");
            });
        }

        @Test
        void systemTimeAsOfFilterAffectsResults() {
            final Date time = new Date(2020, 1, 10, 0, 0, 0);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            final QueryBuilder<Trade> queryBuilder = QueryBuilder.select(Trade.class);
            final List<Trade> results = queryBuilder
                    .systemTimeAsOf(time)
                    .execute(entityManager)
                    .getResults();

            assertThat(results).isNotNull().hasSize(2);
            results.forEach(x -> {
                assertThat(x.getSystemTimeStart()).isBeforeOrEqualTo("2020-01-10 00:00:00.000000");
                assertThat(x.getSystemTimeEnd()).isAfter("2020-01-10 00:00:00.000000");
            });
        }

        @Test
        void validTimeContainsFilterAffectsResults() {
            final LocalDate startDate = LocalDate.of(2020, 1, 10);
            final LocalDate endDate = LocalDate.of(2020, 1, 20);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 10))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 15))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 15))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                            .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 10))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 10))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 9))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 15))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 15))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            final QueryBuilder<Trade> queryBuilder = QueryBuilder.select(Trade.class);
            final List<Trade> results = queryBuilder
                    .validTimeContains(startDate, endDate)
                    .execute(entityManager)
                    .getResults();

            assertThat(results).isNotNull().hasSize(3);
            results.forEach(x -> {
                assertThat(x.getValidTimeStart()).isAfterOrEqualTo(startDate);
                assertThat(x.getValidTimeEnd()).isBeforeOrEqualTo(endDate);
            });
        }

        @Test
        void validTimeEqualsFilterAffectsResults() {
            final LocalDate startDate = LocalDate.of(2020, 1, 10);
            final LocalDate endDate = LocalDate.of(2020, 1, 20);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 10))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 15))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 15))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                            .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 10))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 9))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 15))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            final QueryBuilder<Trade> queryBuilder = QueryBuilder.select(Trade.class);
            final List<Trade> results = queryBuilder
                    .validTimeEquals(startDate, endDate)
                    .execute(entityManager)
                    .getResults();

            assertThat(results).isNotNull().hasSize(1);
            results.forEach(x -> {
                assertThat(x.getValidTimeStart()).isEqualTo(startDate);
                assertThat(x.getValidTimeEnd()).isEqualTo(endDate);
            });
        }

        @Test
        void validTimePrecedesFilterAffectsResults() {
            final LocalDate startDate = LocalDate.of(2020, 1, 10);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 9))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 10))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 8))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 9))
                            .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 9))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 10))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 10))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 10))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            final QueryBuilder<Trade> queryBuilder = QueryBuilder.select(Trade.class);
            final List<Trade> results = queryBuilder
                    .validTimePrecedes(startDate)
                    .execute(entityManager)
                    .getResults();

            assertThat(results).isNotNull().hasSize(3);
            results.forEach(x -> {
                assertThat(x.getValidTimeStart()).isBeforeOrEqualTo(startDate);
                assertThat(x.getValidTimeEnd()).isBeforeOrEqualTo(startDate);
            });
        }

        @Test
        void validTimeImmediatelyPrecedesFilterAffectsResults() {
            final LocalDate startDate = LocalDate.of(2020, 1, 10);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 9))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 10))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 9))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 9))
                            .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 7))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 8))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 7))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            final QueryBuilder<Trade> queryBuilder = QueryBuilder.select(Trade.class);
            final List<Trade> results = queryBuilder
                    .validTimeImmediatelyPrecedes(startDate)
                    .execute(entityManager)
                    .getResults();

            assertThat(results).isNotNull().hasSize(1);
            results.forEach(x -> {
                assertThat(x.getValidTimeStart()).isBeforeOrEqualTo(startDate);
                assertThat(x.getValidTimeEnd()).isBeforeOrEqualTo(startDate);
            });
        }

        @Test
        void validTimeSucceedsFilterAffectsResults() {
            final LocalDate startDate = LocalDate.of(2020, 1, 10);
            final LocalDate endDate = LocalDate.of(2020, 1, 20);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 19))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 21))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 10))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            final QueryBuilder<Trade> queryBuilder = QueryBuilder.select(Trade.class);
            final List<Trade> results = queryBuilder
                    .validTimeSucceeds(endDate)
                    .execute(entityManager)
                    .getResults();

            assertThat(results).isNotNull().hasSize(2);
            results.forEach(x -> {
                assertThat(x.getValidTimeStart()).isAfterOrEqualTo(endDate);
                assertThat(x.getValidTimeEnd()).isAfter(endDate);
            });
        }

        @Test
        void validTimeImmediatelySucceedsFilterAffectsResults() {
            final LocalDate endDate = LocalDate.of(2020, 1, 20);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 19))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 21))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 10))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                            .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            assertThat(
                    QueryBuilder.select(Trade.class)
                            .validTimeImmediatelySucceeds(endDate)
                            .execute(entityManager)
                            .getResults())
                    .isNotNull()
                    .hasSize(1)
                    .first()
                    .satisfies(x -> {
                        assertThat(x.getValidTimeStart()).isEqualTo(endDate);
                        assertThat(x.getValidTimeEnd()).isAfter(endDate);
                    });
        }

        @Test
        void canRetrieveTradesOverlappingValidTimeRange() {
            final LocalDate startDate = LocalDate.of(2020, 1, 15);
            final LocalDate endDate = LocalDate.of(2020, 1, 17);
            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final Trade trade4 = new Trade();
            final Trade trade5 = new Trade();
            final Trade trade6 = new Trade();
            final Trade trade7 = new Trade();
            final Trade trade8 = new Trade();
            final Trade trade9 = new Trade();
            final Trade trade10 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key4 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key5 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key6 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key7 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key8 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key9 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key10 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();

            //x overlaps y
            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 14))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 16));

            //y overlaps x
            trade2.setTradeKey(key2)
                    .setStock("AAPL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 16))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 18));

            //y validTimeContains x
            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 16))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 16));

            //x validTimeContains y
            trade4.setTradeKey(key4)
                    .setStock("NVDA")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 14))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 18));

            //x starts y
            trade5.setTradeKey(key5)
                    .setStock("EBAY")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 18));

            //x finishes y
            trade6.setTradeKey(key6)
                    .setStock("FB")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 14))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 17));

            //y starts x
            trade7.setTradeKey(key7)
                    .setStock("AMD")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 16));

            //y finishes x
            trade8.setTradeKey(key8)
                    .setStock("GSK")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 16))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 17));

            //x equals y
            trade9.setTradeKey(key9)
                    .setStock("AMZN")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 17));

            trade10.setTradeKey(key10)
                    .setStock("TSLA")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 11))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 12));

            repository.saveAll(
                    ImmutableList.of(trade1, trade2, trade3, trade4, trade5, trade6, trade7, trade8, trade9, trade10));

            final List<Trade> trades = QueryBuilder.select(Trade.class)
                    .validTimeOverlaps(startDate, endDate)
                    .execute(entityManager)
                    .getResults();

            assertThat(trades).isNotNull().hasSize(9);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

            assertThat(trades.get(1)).isNotNull()
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 16))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 18));

            assertThat(trades.get(2)).isNotNull()
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 16))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

            assertThat(trades.get(3)).isNotNull()
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 18));

            assertThat(trades.get(4)).isNotNull()
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 15))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 18));

            assertThat(trades.get(5)).isNotNull()
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 17));

            assertThat(trades.get(6)).isNotNull()
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 15))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

            assertThat(trades.get(7)).isNotNull()
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 16))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 17));

            assertThat(trades.get(8)).isNotNull()
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 15))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 17));
        }

        @Test
        void canCreateDistinctQuery() {
            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 19))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 21))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

//            QueryBuilder.selectDistinct(Trade.class).
            //TODO: finish this
        }
    }

    @DataJpaTest
    @SpringJUnitConfig
    @Nested
    class insertQueries {

        @Autowired
        private TradeReadRepository readRepository;

        @Autowired
        private TradeWriteRepository writeRepository;

        @PersistenceContext
        private EntityManager entityManager;

        @Test
        void canCreateQueryForInsert() {
            assertThat(QueryBuilder.insert(Trade.class)).isNotNull()
                    .hasFieldOrPropertyWithValue("query", new Query<>(QueryType.INSERT, Trade.class))
                    .hasFieldOrPropertyWithValue("queryClass", Trade.class);
        }

        @Test
        void insertFromObjectInsertsObject() {
            final BitemporalKey key = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build();
            final Trade trade = new Trade().setTradeKey(key)
                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                    .setSystemTimeStart(new Date(2020, 1, 20, 3, 45, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 45, 0))
                    .setVolume(200)
                    .setPrice(new BigDecimal("123.45"))
                    .setMarketLimitFlag('M')
                    .setBuySellFlag('B')
                    .setStock("GOOGL");

            QueryBuilder.insert(Trade.class).from(trade).execute(entityManager);

            final Trade retrievedTrade = readRepository.getOne(key);
            assertThat(retrievedTrade).isNotNull().usingRecursiveComparison().isEqualTo(trade);
        }
    }
}

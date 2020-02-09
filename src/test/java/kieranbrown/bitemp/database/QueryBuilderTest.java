package kieranbrown.bitemp.database;

import com.google.common.collect.ImmutableList;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
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
            assertThat(select(Trade.class)).isNotNull()
                    .hasFieldOrPropertyWithValue("query", new Query<>(QueryType.SELECT, Trade.class))
                    .hasFieldOrPropertyWithValue("queryClass", Trade.class);
        }

        @Test
        void allFieldsSetsAllFieldsFromTheSourceObject() {
            final Map<String, Object> fields = HashMap.ofEntries(
                    new Tuple2<>("id", null),
                    new Tuple2<>("version", null),
                    new Tuple2<>("valid_time_start", null),
                    new Tuple2<>("valid_time_end", null),
                    new Tuple2<>("system_time_start", null),
                    new Tuple2<>("system_time_end", null),
                    new Tuple2<>("stock", null),
                    new Tuple2<>("price", null),
                    new Tuple2<>("volume", null),
                    new Tuple2<>("buy_sell_flag", null),
                    new Tuple2<>("market_limit_flag", null)
            );

            System.out.println("test fields");
            fields.keySet().forEach(System.out::println);

            final QueryBuilder queryBuilder = QueryBuilder.selectDistinct(Trade.class).allFields();
            queryBuilder.execute(entityManager);

            assertThat(queryBuilder).isNotNull()
                    .extracting("query")
                    .extracting("fields")
                    .isEqualTo(fields);
        }

        @Test
        void canEvaluateSingleSelectQueryWithAllFieldsAndReturnResult() {
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
            final QueryBuilder queryBuilder = QueryBuilder.selectDistinct(Trade.class).allFields();
            queryBuilder.execute(entityManager);
            final List results = queryBuilder.getResults();
            assertThat(results).isNotNull()
                    .isNotEmpty();

            assertThat(results.get(0)).usingRecursiveComparison().isEqualTo(trade);

            //TODO: finish
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
            final List<Trade> results = queryBuilder.allFields()
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
            final List<Trade> results = queryBuilder.allFields()
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
            final List<Trade> results = queryBuilder.allFields()
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
            final List<Trade> results = queryBuilder.allFields()
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
            final List<Trade> results = queryBuilder.allFields()
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
            final LocalDate endDate = LocalDate.of(2020, 1, 20);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 9))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                            .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                            .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 9))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 19))
                            .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                            .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                            .setValidTimeStart(LocalDate.of(2020, 1, 9))
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
            final List<Trade> results = queryBuilder.allFields()
                    .validTimePrecedes(startDate, endDate)
                    .execute(entityManager)
                    .getResults();

            assertThat(results).isNotNull().hasSize(2);
            results.forEach(x -> {
                assertThat(x.getValidTimeStart()).isBefore(startDate);
                assertThat(x.getValidTimeEnd()).isBeforeOrEqualTo(endDate);
            });
        }
    }
}

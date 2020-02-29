package kieranbrown.bitemp.database;

import com.google.common.collect.ImmutableList;
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
import java.time.LocalDateTime;
import java.util.UUID;

import static kieranbrown.bitemp.database.QueryBuilder.select;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryBuilderTest {

    @SpringJUnitConfig
    @DataJpaTest
    @Nested
    class SelectQueries {
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
            final Trade trade = new Trade().setTradeKey(
                    new BitemporalKey.Builder()
                            .setTradeId(UUID.randomUUID())
                            .setValidTimeStart(LocalDate.of(2020, 1, 20))
                            .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                            .build())
                    .setSystemTimeStart(LocalDateTime.of(2020, 1, 20, 3, 45, 0))
                    .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 20, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 21))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 22))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 20, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 22))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 23))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 20, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                            .setVolume(195)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL")
            ));

            assertThat(
                    QueryBuilder.select(Trade.class)
                            .where("valid_time_start", QueryEquality.EQUALS, LocalDate.of(2020, 1, 20))
                            .execute(entityManager)
                            .getResults())
                    .isNotNull()
                    .hasSize(1);

            assertThat(
                    QueryBuilder.select(Trade.class)
                            .where(new SingleQueryFilter("valid_time_start", QueryEquality.EQUALS, LocalDate.of(2020, 1, 20)))
                            .execute(entityManager)
                            .getResults())
                    .isNotNull()
                    .hasSize(1);

            assertThat(
                    QueryBuilder.select(Trade.class)
                            .where(new SingleQueryFilter("stock", QueryEquality.EQUALS, "GOOGL"),
                                    new SingleQueryFilter("volume", QueryEquality.EQUALS, 195))
                            .execute(entityManager)
                            .getResults())
                    .isNotNull()
                    .hasSize(1);
        }

        @Test
        void systemTimeBetweenFilterAffectsResults() {
            final LocalDateTime startRange = LocalDateTime.of(2020, 1, 10, 0, 0, 0);
            final LocalDateTime endRange = LocalDateTime.of(2020, 1, 20, 0, 0, 0);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 12, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 20, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                assertThat(x.getSystemTimeStart()).isAfterOrEqualTo(startRange);
                assertThat(x.getSystemTimeEnd()).isBeforeOrEqualTo(endRange);
            });
        }

        @Test
        void systemTimeFromFilterAffectsResults() {
            final LocalDateTime startRange = LocalDateTime.of(2020, 1, 10, 0, 0, 0);
            final LocalDateTime endRange = LocalDateTime.of(2020, 1, 20, 0, 0, 0);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 12, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 20, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                assertThat(x.getSystemTimeStart()).isAfterOrEqualTo(startRange);
                assertThat(x.getSystemTimeEnd()).isBefore(endRange);
            });
        }

        @Test
        void systemTimeAsOfFilterAffectsResults() {
            final LocalDateTime time = LocalDateTime.of(2020, 1, 10, 0, 0, 0);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                assertThat(x.getSystemTimeStart()).isBeforeOrEqualTo(time);
                assertThat(x.getSystemTimeEnd()).isAfter(time);
            });
        }

        @Test
        void validTimeContainsFilterAffectsResults() {
            final LocalDate startDate = LocalDate.of(2020, 1, 10);
            final LocalDate endDate = LocalDate.of(2020, 1, 20);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 15))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 10))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 15))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                assertThat(x.getTradeKey().getValidTimeStart()).isAfterOrEqualTo(startDate);
                assertThat(x.getTradeKey().getValidTimeEnd()).isBeforeOrEqualTo(endDate);
            });
        }

        @Test
        void validTimeEqualsFilterAffectsResults() {
            final LocalDate startDate = LocalDate.of(2020, 1, 10);
            final LocalDate endDate = LocalDate.of(2020, 1, 20);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 15))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 15))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                assertThat(x.getTradeKey().getValidTimeStart()).isEqualTo(startDate);
                assertThat(x.getTradeKey().getValidTimeEnd()).isEqualTo(endDate);
            });
        }

        @Test
        void validTimePrecedesFilterAffectsResults() {
            final LocalDate startDate = LocalDate.of(2020, 1, 10);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 10))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 8))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 9))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 10))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                assertThat(x.getTradeKey().getValidTimeStart()).isBeforeOrEqualTo(startDate);
                assertThat(x.getTradeKey().getValidTimeEnd()).isBeforeOrEqualTo(startDate);
            });
        }

        @Test
        void validTimeImmediatelyPrecedesFilterAffectsResults() {
            final LocalDate startDate = LocalDate.of(2020, 1, 10);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 10))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 9))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 7))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 8))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 7))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                assertThat(x.getTradeKey().getValidTimeStart()).isBeforeOrEqualTo(startDate);
                assertThat(x.getTradeKey().getValidTimeEnd()).isBeforeOrEqualTo(startDate);
            });
        }

        @Test
        void validTimeSucceedsFilterAffectsResults() {
            final LocalDate endDate = LocalDate.of(2020, 1, 20);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 19))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 21))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                assertThat(x.getTradeKey().getValidTimeStart()).isAfterOrEqualTo(endDate);
                assertThat(x.getTradeKey().getValidTimeEnd()).isAfter(endDate);
            });
        }

        @Test
        void validTimeImmediatelySucceedsFilterAffectsResults() {
            final LocalDate endDate = LocalDate.of(2020, 1, 20);

            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 19))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 21))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 19, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
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
                        assertThat(x.getTradeKey().getValidTimeStart()).isEqualTo(endDate);
                        assertThat(x.getTradeKey().getValidTimeEnd()).isAfter(endDate);
                    });
        }

        @Test
        void canRetrieveTradesOverlappingValidTimeRange() {
            final LocalDate startDate = LocalDate.of(2020, 1, 15);
            final LocalDate endDate = LocalDate.of(2020, 1, 17);

            repository.saveAll(ImmutableList.of(
                    //x overlaps y
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 14))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 16))
                                    .build())
                            .setStock("GOOGL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("100.127"))
                            .setVolume(200)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0)),

                    //y overlaps x
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 16))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 18))
                                    .build())
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("189.213"))
                            .setVolume(195)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0)),

                    //y validTimeContains x
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 16))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 16))
                                    .build())
                            .setStock("MSFT")
                            .setBuySellFlag('S')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("78.345"))
                            .setVolume(199)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 15, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 30, 0)),

                    //x validTimeContains y
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 14))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 18))
                                    .build())
                            .setStock("NVDA")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("78.345"))
                            .setVolume(199)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 15, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 30, 0)),

                    //x starts y
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 18))
                                    .build())
                            .setStock("EBAY")
                            .setBuySellFlag('S')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("78.345"))
                            .setVolume(199)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 15, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 30, 0)),

                    //x finishes y
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 14))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 17))
                                    .build())
                            .setStock("FB")
                            .setBuySellFlag('S')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("78.345"))
                            .setVolume(199)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 15, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 30, 0)),

                    //y starts x
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 16))
                                    .build())
                            .setStock("AMD")
                            .setBuySellFlag('S')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("78.345"))
                            .setVolume(199)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 15, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 30, 0)),

                    //y finishes x
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 16))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 17))
                                    .build())
                            .setStock("GSK")
                            .setBuySellFlag('S')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("78.345"))
                            .setVolume(199)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 15, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 30, 0)),

                    //x equals y
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 17))
                                    .build())
                            .setStock("AMZN")
                            .setBuySellFlag('S')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("78.345"))
                            .setVolume(199)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 15, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 30, 0)),

                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 11))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 12))
                                    .build())
                            .setStock("TSLA")
                            .setBuySellFlag('S')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("78.345"))
                            .setVolume(199)
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 15, 10, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 30, 0))
                    )
            );

            final List<Trade> trades = QueryBuilder.select(Trade.class)
                    .validTimeOverlaps(startDate, endDate)
                    .execute(entityManager)
                    .getResults();

            assertThat(trades).isNotNull().hasSize(9);

            assertThat(trades.get(0)).isNotNull()
                    .extracting("tradeKey")
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

            assertThat(trades.get(1)).isNotNull()
                    .extracting("tradeKey")
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 16))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 18));

            assertThat(trades.get(2)).isNotNull()
                    .extracting("tradeKey")
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 16))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

            assertThat(trades.get(3)).isNotNull()
                    .extracting("tradeKey")
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 18));

            assertThat(trades.get(4)).isNotNull()
                    .extracting("tradeKey")
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 15))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 18));

            assertThat(trades.get(5)).isNotNull()
                    .extracting("tradeKey")
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 17));

            assertThat(trades.get(6)).isNotNull()
                    .extracting("tradeKey")
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 15))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

            assertThat(trades.get(7)).isNotNull()
                    .extracting("tradeKey")
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 16))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 17));

            assertThat(trades.get(8)).isNotNull()
                    .extracting("tradeKey")
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 15))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 17));
        }

        @Test
        void canCreateDistinctQuery() {
            repository.saveAll(ImmutableList.of(
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 11, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 19))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 3, 45, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 13, 3, 45, 0))
                            .setVolume(200)
                            .setPrice(new BigDecimal("123.45"))
                            .setMarketLimitFlag('M')
                            .setBuySellFlag('B')
                            .setStock("GOOGL"),
                    new Trade().setTradeKey(
                            new BitemporalKey.Builder()
                                    .setTradeId(UUID.randomUUID())
                                    .setValidTimeStart(LocalDate.of(2020, 1, 21))
                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 1, 10, 0, 0, 0))
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
            final UUID tradeId = UUID.randomUUID();
            final BitemporalKey key = new BitemporalKey.Builder()
                    .setTradeId(tradeId)
                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                    .build();
            final Trade trade = new Trade().setTradeKey(key)
                    .setSystemTimeStart(LocalDateTime.of(2020, 1, 20, 3, 45, 0))
                    .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                    .setVolume(200)
                    .setPrice(new BigDecimal("123.45"))
                    .setMarketLimitFlag('M')
                    .setBuySellFlag('B')
                    .setStock("GOOGL");

            QueryBuilder.insert(Trade.class).from(trade).execute(entityManager);

            assertThat(
                    entityManager.createNativeQuery("select * from reporting.trade_data where valid_time_start = ?1 and valid_time_end = ?2 and id = ?3")
                            .setParameter(1, LocalDate.of(2020, 1, 20))
                            .setParameter(2, LocalDate.of(2020, 1, 21))
                            .setParameter(3, tradeId)
                            .getResultList()
                            .get(0))
                    .isNotNull()
                    .usingRecursiveComparison()
                    .isEqualTo(trade);
        }
    }
}

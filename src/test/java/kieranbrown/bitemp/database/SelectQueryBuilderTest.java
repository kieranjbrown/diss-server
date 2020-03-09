package kieranbrown.bitemp.database;

import io.vavr.collection.List;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@DataJpaTest
@SpringJUnitConfig
class SelectQueryBuilderTest {
    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    private DataSource dataSource;

    @Test
    void canEvaluateSingleSelectQueryAndReturnResult() throws OverlappingKeyException {
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

        QueryBuilderFactory.insert(Trade.class)
                .from(trade)
                .execute(dataSource, entityManager);

        assertThat(
                QueryBuilderFactory.selectDistinct(Trade.class)
                        .execute(entityManager)
                        .getResults())
                .isNotNull()
                .isNotEmpty()
                .first()
                .usingRecursiveComparison()
                .isEqualTo(trade);
    }

    @Test
    void throwsIfResultsAreRetrievedBeforeCodeIsExecuted() {
        assertThat(assertThrows(IllegalStateException.class, () -> QueryBuilderFactory.select(Trade.class).getResults()))
                .hasMessage("call to getResults before executing query");
    }

    @Test
    void settingFilterAffectsQuery() throws OverlappingKeyException {
        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        assertThat(
                QueryBuilderFactory.select(Trade.class)
                        .where("valid_time_start", QueryEquality.EQUALS, LocalDate.of(2020, 1, 20))
                        .execute(entityManager)
                        .getResults())
                .isNotNull()
                .hasSize(1);

        assertThat(
                QueryBuilderFactory.select(Trade.class)
                        .where(new SingleQueryFilter("valid_time_start", QueryEquality.EQUALS, LocalDate.of(2020, 1, 20)))
                        .execute(entityManager)
                        .getResults())
                .isNotNull()
                .hasSize(1);

        assertThat(
                QueryBuilderFactory.select(Trade.class)
                        .where(new SingleQueryFilter("stock", QueryEquality.EQUALS, "GOOGL"),
                                new SingleQueryFilter("volume", QueryEquality.EQUALS, 195))
                        .execute(entityManager)
                        .getResults())
                .isNotNull()
                .hasSize(1);
    }

    @Test
    void systemTimeBetweenFilterAffectsResults() throws OverlappingKeyException {
        final LocalDateTime startRange = LocalDateTime.of(2020, 1, 10, 0, 0, 0);
        final LocalDateTime endRange = LocalDateTime.of(2020, 1, 20, 0, 0, 0);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        final SelectQueryBuilder<Trade> selectQueryBuilder = QueryBuilderFactory.select(Trade.class);
        final List<Trade> results = selectQueryBuilder
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
    void systemTimeFromFilterAffectsResults() throws OverlappingKeyException {
        final LocalDateTime startRange = LocalDateTime.of(2020, 1, 10, 0, 0, 0);
        final LocalDateTime endRange = LocalDateTime.of(2020, 1, 20, 0, 0, 0);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        final SelectQueryBuilder<Trade> selectQueryBuilder = QueryBuilderFactory.select(Trade.class);
        final List<Trade> results = selectQueryBuilder
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
    void systemTimeAsOfFilterAffectsResults() throws OverlappingKeyException {
        final LocalDateTime time = LocalDateTime.of(2020, 1, 10, 0, 0, 0);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        final SelectQueryBuilder<Trade> selectQueryBuilder = QueryBuilderFactory.select(Trade.class);
        final List<Trade> results = selectQueryBuilder
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
    void validTimeContainsFilterAffectsResults() throws OverlappingKeyException {
        final LocalDate startDate = LocalDate.of(2020, 1, 10);
        final LocalDate endDate = LocalDate.of(2020, 1, 20);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        final SelectQueryBuilder<Trade> selectQueryBuilder = QueryBuilderFactory.select(Trade.class);
        final List<Trade> results = selectQueryBuilder
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
    void validTimeEqualsFilterAffectsResults() throws OverlappingKeyException {
        final LocalDate startDate = LocalDate.of(2020, 1, 10);
        final LocalDate endDate = LocalDate.of(2020, 1, 20);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        final SelectQueryBuilder<Trade> selectQueryBuilder = QueryBuilderFactory.select(Trade.class);
        final List<Trade> results = selectQueryBuilder
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
    void validTimePrecedesFilterAffectsResults() throws OverlappingKeyException {
        final LocalDate startDate = LocalDate.of(2020, 1, 10);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        final SelectQueryBuilder<Trade> selectQueryBuilder = QueryBuilderFactory.select(Trade.class);
        final List<Trade> results = selectQueryBuilder
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
    void validTimeImmediatelyPrecedesFilterAffectsResults() throws OverlappingKeyException {
        final LocalDate startDate = LocalDate.of(2020, 1, 10);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        final SelectQueryBuilder<Trade> selectQueryBuilder = QueryBuilderFactory.select(Trade.class);
        final List<Trade> results = selectQueryBuilder
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
    void validTimeSucceedsFilterAffectsResults() throws OverlappingKeyException {
        final LocalDate endDate = LocalDate.of(2020, 1, 20);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        final SelectQueryBuilder<Trade> selectQueryBuilder = QueryBuilderFactory.select(Trade.class);
        final List<Trade> results = selectQueryBuilder
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
    void validTimeImmediatelySucceedsFilterAffectsResults() throws OverlappingKeyException {
        final LocalDate endDate = LocalDate.of(2020, 1, 20);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);

        assertThat(
                QueryBuilderFactory.select(Trade.class)
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
    void canRetrieveTradesOverlappingValidTimeRange() throws OverlappingKeyException {
        final LocalDate startDate = LocalDate.of(2020, 1, 15);
        final LocalDate endDate = LocalDate.of(2020, 1, 17);

        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
                        //S1 > S2 AND NOT (S1 >= E2 AND E1 >= E2)
                        new Trade().setTradeKey(
                                new BitemporalKey.Builder()
                                        .setTradeId(UUID.randomUUID())
                                        .setValidTimeStart(LocalDate.of(2020, 1, 16))
                                        .setValidTimeEnd(LocalDate.of(2020, 1, 16))
                                        .build())
                                .setStock("GOOGL")
                                .setBuySellFlag('B')
                                .setMarketLimitFlag('M')
                                .setPrice(new BigDecimal("100.127"))
                                .setVolume(200)
                                .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0)),
                        new Trade().setTradeKey(
                                new BitemporalKey.Builder()
                                        .setTradeId(UUID.randomUUID())
                                        .setValidTimeStart(LocalDate.of(2020, 1, 16))
                                        .setValidTimeEnd(LocalDate.of(2020, 1, 18))
                                        .build())
                                .setStock("GOOGL")
                                .setBuySellFlag('B')
                                .setMarketLimitFlag('M')
                                .setPrice(new BigDecimal("100.127"))
                                .setVolume(200)
                                .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0)),
                        new Trade().setTradeKey(
                                new BitemporalKey.Builder()
                                        .setTradeId(UUID.randomUUID())
                                        .setValidTimeStart(LocalDate.of(2020, 1, 18))
                                        .setValidTimeEnd(LocalDate.of(2020, 1, 19))
                                        .build())
                                .setStock("GOOGL")
                                .setBuySellFlag('B')
                                .setMarketLimitFlag('M')
                                .setPrice(new BigDecimal("100.127"))
                                .setVolume(200)
                                .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0)),
                        //S2 > S1 AND NOT (S2 >= E1 AND E2 >= E1)
                        new Trade().setTradeKey(
                                new BitemporalKey.Builder()
                                        .setTradeId(UUID.randomUUID())
                                        .setValidTimeStart(LocalDate.of(2020, 1, 14))
                                        .setValidTimeEnd(LocalDate.of(2020, 1, 18))
                                        .build())
                                .setStock("GOOGL")
                                .setBuySellFlag('B')
                                .setMarketLimitFlag('M')
                                .setPrice(new BigDecimal("100.127"))
                                .setVolume(200)
                                .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0)),
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
                        new Trade().setTradeKey(
                                new BitemporalKey.Builder()
                                        .setTradeId(UUID.randomUUID())
                                        .setValidTimeStart(LocalDate.of(2020, 1, 13))
                                        .setValidTimeEnd(LocalDate.of(2020, 1, 15))
                                        .build())
                                .setStock("GOOGL")
                                .setBuySellFlag('B')
                                .setMarketLimitFlag('M')
                                .setPrice(new BigDecimal("100.127"))
                                .setVolume(200)
                                .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0)),
                        //S1 = S2 AND (E1 = E2 OR E1 <> E2)
                        new Trade().setTradeKey(
                                new BitemporalKey.Builder()
                                        .setTradeId(UUID.randomUUID())
                                        .setValidTimeStart(LocalDate.of(2020, 1, 15))
                                        .setValidTimeEnd(LocalDate.of(2020, 1, 17))
                                        .build())
                                .setStock("GOOGL")
                                .setBuySellFlag('B')
                                .setMarketLimitFlag('M')
                                .setPrice(new BigDecimal("100.127"))
                                .setVolume(200)
                                .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0)),
                        new Trade().setTradeKey(
                                new BitemporalKey.Builder()
                                        .setTradeId(UUID.randomUUID())
                                        .setValidTimeStart(LocalDate.of(2020, 1, 15))
                                        .setValidTimeEnd(LocalDate.of(2020, 1, 19))
                                        .build())
                                .setStock("GOOGL")
                                .setBuySellFlag('B')
                                .setMarketLimitFlag('M')
                                .setPrice(new BigDecimal("100.127"))
                                .setVolume(200)
                                .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0))
                        )
                )
                .execute(dataSource, entityManager);

        final List<Trade> trades = QueryBuilderFactory.select(Trade.class)
                .validTimeOverlaps(startDate, endDate)
                .execute(entityManager)
                .getResults();

        assertThat(trades).isNotNull().hasSize(6);

        assertThat(trades.get(0)).isNotNull()
                .extracting("tradeKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 16))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

        assertThat(trades.get(1)).isNotNull()
                .extracting("tradeKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 16))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 18));

        assertThat(trades.get(2)).isNotNull()
                .extracting("tradeKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 18));

        assertThat(trades.get(3)).isNotNull()
                .extracting("tradeKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

        assertThat(trades.get(4)).isNotNull()
                .extracting("tradeKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 15))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 17));

        assertThat(trades.get(5)).isNotNull()
                .extracting("tradeKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 15))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 19));
    }

    @Test
    void canCreateDistinctQuery() throws OverlappingKeyException {
        QueryBuilderFactory.insert(Trade.class)
                .fromAll(List.of(
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
                ))
                .execute(dataSource, entityManager);
        fail();
//            QueryBuilderFactory.selectDistinct(Trade.class).
        //TODO: finish this
    }
}

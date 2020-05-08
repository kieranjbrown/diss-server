package kieranbrown.bitemp.database;

import io.vavr.collection.List;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.BeforeEach;
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

@DataJpaTest
@SpringJUnitConfig
class DeleteQueryBuilderTest {
    @Autowired
    private DataSource dataSource;

    @PersistenceContext
    private EntityManager entityManager;

    @BeforeEach
    void setup() {
        entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
    }

    @Test
    void constructorThrowsForNullInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new DeleteQueryBuilder<>(null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void executeDeletesRowsInDatabase() throws OverlappingKeyException, InvalidPeriodException {
        final Trade trade1 = new Trade().setBitemporalKey(
                new BitemporalKey.Builder()
                        .setTradeId(UUID.randomUUID())
                        .setValidTimeStart(LocalDate.of(2020, 1, 14))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 16))
                        .build())
                .setStock("AAPL")
                .setBuySellFlag('B')
                .setMarketLimitFlag('M')
                .setPrice(new BigDecimal("123.45"))
                .setVolume(200)
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0));

        final Trade trade2 = new Trade().setBitemporalKey(
                new BitemporalKey.Builder()
                        .setTradeId(UUID.randomUUID())
                        .setValidTimeStart(LocalDate.of(2020, 1, 16))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 18))
                        .build())
                .setStock("AAPL")
                .setBuySellFlag('B')
                .setMarketLimitFlag('M')
                .setPrice(new BigDecimal("123.45"))
                .setVolume(195)
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 9, 10, 0, 0))
                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0));

        final Trade trade3 = new Trade().setBitemporalKey(
                new BitemporalKey.Builder()
                        .setTradeId(UUID.randomUUID())
                        .setValidTimeStart(LocalDate.of(2020, 1, 16))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 16))
                        .build())
                .setStock("AAPL")
                .setBuySellFlag('S')
                .setMarketLimitFlag('M')
                .setPrice(new BigDecimal("78.345"))
                .setVolume(199)
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 15, 10, 0, 0))
                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 30, 0));

        new InsertQueryBuilder<>(Trade.class)
                .fromAll(List.of(trade1, trade2, trade3))
                .execute(entityManager);

        QueryBuilderFactory.delete(Trade.class)
                .where(new SingleQueryFilter("price", QueryEquality.EQUALS, new BigDecimal("123.45")))
                .execute(entityManager);

        final List<Trade> results = QueryBuilderFactory.select(Trade.class)
                .where(new SingleQueryFilter("stock", QueryEquality.EQUALS, "AAPL"))
                .execute(entityManager)
                .getResults();

        assertThat(results).hasSize(1);

        results.forEach(x -> assertThat(x.getPrice()).isNotEqualTo(new BigDecimal("123.45")));
    }

    @Test
    void deleteForValidTimePeriod() throws OverlappingKeyException, InvalidPeriodException {
        final UUID tradeId = UUID.randomUUID();
        new InsertQueryBuilder<>(Trade.class).from(new Trade().setBitemporalKey(
                new BitemporalKey.Builder()
                        .setTradeId(tradeId)
                        .setValidTimeStart(LocalDate.of(2020, 1, 14))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                        .build())
                .setStock("AAPL")
                .setBuySellFlag('B')
                .setMarketLimitFlag('M')
                .setPrice(new BigDecimal("123.45"))
                .setVolume(200)
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0))
        ).execute(entityManager);

        new DeleteQueryBuilder<>(Trade.class)
                .forValidTimePeriod(LocalDate.of(2020, 1, 16), LocalDate.of(2020, 1, 18))
                .where(new SingleQueryFilter("id", QueryEquality.EQUALS, tradeId))
                .execute(entityManager);

        final List<Trade> trades = new SelectQueryBuilder<>(Trade.class)
                .where(new SingleQueryFilter("id", QueryEquality.EQUALS, tradeId))
                .execute(entityManager)
                .getResults();

        assertThat(trades).hasSize(2);

        assertThat(trades.get(0))
                .extracting("bitemporalKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

        assertThat(trades.get(1))
                .extracting("bitemporalKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 18))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 20));
    }

    @Test
    void deleteForValidTimePeriodAffectsOnlyThoseThatNeedIt() throws OverlappingKeyException, InvalidPeriodException {
        final UUID id1 = UUID.fromString("769fb864-f3b7-4ca5-965e-bcff80088197");
        final UUID id2 = UUID.fromString("769fb864-f3b7-4ca5-965e-bcff80088198");
        new InsertQueryBuilder<>(Trade.class).fromAll(new Trade().setBitemporalKey(
                new BitemporalKey.Builder()
                        .setTradeId(id1)
                        .setValidTimeStart(LocalDate.of(2020, 1, 14))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                        .build())
                        .setStock("AAPL")
                        .setBuySellFlag('B')
                        .setMarketLimitFlag('M')
                        .setPrice(new BigDecimal("123.45"))
                        .setVolume(200)
                        .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                        .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0)),
                new Trade().setBitemporalKey(
                        new BitemporalKey.Builder()
                                .setTradeId(id2)
                                .setValidTimeStart(LocalDate.of(2020, 1, 10))
                                .setValidTimeEnd(LocalDate.of(2020, 1, 16))
                                .build())
                        .setStock("MSFT")
                        .setBuySellFlag('B')
                        .setMarketLimitFlag('M')
                        .setPrice(new BigDecimal("123.45"))
                        .setVolume(200)
                        .setSystemTimeStart(LocalDateTime.of(2020, 1, 10, 10, 0, 0))
                        .setSystemTimeEnd(LocalDateTime.of(2020, 1, 15, 3, 30, 0))
        ).execute(entityManager);

        new DeleteQueryBuilder<>(Trade.class)
                .forValidTimePeriod(LocalDate.of(2020, 1, 16), LocalDate.of(2020, 1, 18))
                .where(new SingleQueryFilter("stock", QueryEquality.EQUALS, "AAPL"))
                .execute(entityManager);

        final List<Trade> trades = new SelectQueryBuilder<>(Trade.class)
                .execute(entityManager)
                .getResults()
                .sortBy(x -> x.getBitemporalKey().getId());

        assertThat(trades).hasSize(4);

        trades.forEach(System.out::println);

        assertThat(trades.get(0))
                .hasFieldOrPropertyWithValue("stock", "AAPL")
                .extracting("bitemporalKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

        assertThat(trades.get(1))
                .hasFieldOrPropertyWithValue("stock", "MSFT")
                .extracting("bitemporalKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 10))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

        assertThat(trades.get(2))
                .hasFieldOrPropertyWithValue("stock", "AAPL")
                .extracting("bitemporalKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 18))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 20));
    }
}

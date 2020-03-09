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

@DataJpaTest
@SpringJUnitConfig
class UpdateQueryBuilderTest {
    @Autowired
    private DataSource dataSource;

    @PersistenceContext
    private EntityManager entityManager;

    @Test
    void constructorThrowsForNullInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new UpdateQueryBuilder<>(null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void executeUpdatesRowsInDatabase() throws OverlappingKeyException {
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

        QueryBuilderFactory.update(Trade.class)
                .set("volume", 250)
                .where(new SingleQueryFilter("price", QueryEquality.EQUALS, new BigDecimal("123.45")))
                .execute(entityManager);

        final List<Trade> results = QueryBuilderFactory.select(Trade.class)
                .where(new SingleQueryFilter("stock", QueryEquality.EQUALS, "AAPL"))
                .execute(entityManager)
                .getResults();

        assertThat(results).hasSize(3);

        results.filter(x -> x.getPrice().equals(new BigDecimal("123.45")))
                .forEach(x -> assertThat(x.getVolume()).isEqualTo(250));
    }

    @Test
    void updateForValidTimePeriod() throws OverlappingKeyException {
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

        new UpdateQueryBuilder<>(Trade.class)
                .forValidTimePeriod(LocalDate.of(2020, 1, 16), LocalDate.of(2020, 1, 18))
                .set("stock", "MSFT")
                .where(new SingleQueryFilter("id", QueryEquality.EQUALS, tradeId))
                .execute(entityManager);

        final List<Trade> trades = new SelectQueryBuilder<>(QueryType.SELECT, Trade.class)
                .where(new SingleQueryFilter("id", QueryEquality.EQUALS, tradeId))
                .execute(entityManager)
                .getResults();

        assertThat(trades).hasSize(3);

        assertThat(trades.get(0))
                .extracting("bitemporalKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 16));

        assertThat(trades.get(1))
                .hasFieldOrPropertyWithValue("stock", "MSFT")
                .extracting("bitemporalKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 16))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 18));

        assertThat(trades.get(2))
                .extracting("bitemporalKey")
                .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 18))
                .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 20));
    }
}

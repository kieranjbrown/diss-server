package kieranbrown.bitemp.database;

import io.vavr.collection.Stream;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DataJpaTest
@SpringJUnitConfig
class InsertQueryBuilderTest {

    //valid time inserts = validate no overlap, and make sure there's continuity?
    //system time inserts = update old system time to now (assume they match up)

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private DataSource dataSource;

    @BeforeEach
    void setup() {
        entityManager.setFlushMode(FlushModeType.AUTO);
        assertThat(dataSource).isNotNull();
    }

    @Test
    void insertFromObjectInsertsObject() throws OverlappingKeyException {
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

        QueryBuilderFactory.insert(Trade.class).from(trade).execute(dataSource, entityManager);

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

    @Test
    void canInsertMultipleObjectsAtOnce() throws OverlappingKeyException {
        final Trade trade1 = new Trade().setTradeKey(new BitemporalKey.Builder()
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

        final Trade trade2 = new Trade().setTradeKey(new BitemporalKey.Builder()
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

        QueryBuilderFactory.insert(Trade.class).fromAll(trade1, trade2).execute(dataSource, entityManager);
        assertTradesAreEqual(trade1, trade2);

        QueryBuilderFactory.insert(Trade.class).fromAll(io.vavr.collection.List.of(trade1, trade2)).execute(dataSource, entityManager);
        assertTradesAreEqual(trade1, trade2);

        QueryBuilderFactory.insert(Trade.class).fromAll(Stream.of(trade1, trade2)).execute(dataSource, entityManager);
        assertTradesAreEqual(trade1, trade2);
    }

    @SuppressWarnings("unchecked")
    private void assertTradesAreEqual(final Trade trade1, final Trade trade2) {
        final List arrayResults = entityManager.createNativeQuery("select * from reporting.trade_data where valid_time_start = ?1 and valid_time_end = ?2")
                .setParameter(1, LocalDate.of(2020, 1, 20))
                .setParameter(2, LocalDate.of(2020, 1, 21))
                .getResultList();
        assertThat(arrayResults).isNotNull().hasSize(2);
        assertThat(arrayResults.get(0)).usingRecursiveComparison().isEqualTo(trade1);
        assertThat(arrayResults.get(1)).usingRecursiveComparison().isEqualTo(trade2);

        entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
    }

    //TODO: overlap doesn't allow this but you should be able to have one day end the same as another start?
    //maybe change query to allow this
    @Test
    void insertFromObjectUpdatesExistingSystemTime() throws OverlappingKeyException {
        final LocalDateTime now = LocalDateTime.now();
        final UUID tradeId = UUID.randomUUID();
        final Trade trade1 = new Trade().setTradeKey(
                new BitemporalKey.Builder()
                        .setTradeId(tradeId)
                        .setValidTimeStart(LocalDate.of(2020, 1, 20))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                        .build())
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 20, 3, 45, 0))
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL");

        final Trade trade2 = new Trade().setTradeKey(
                new BitemporalKey.Builder()
                        .setTradeId(tradeId)
                        .setValidTimeStart(LocalDate.of(2020, 1, 21))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 23))
                        .build())
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL");

        QueryBuilderFactory.insert(Trade.class)
                .from(trade1)
                .execute(dataSource, entityManager);

        assertThat(
                entityManager.createNativeQuery("select * from reporting.trade_data where id = ?1", Trade.class)
                        .setParameter(1, tradeId)
                        .getResultList()
                        .get(0))
                .isNotNull()
                .hasFieldOrPropertyWithValue("systemTimeStart", LocalDateTime.of(2020, 1, 20, 3, 45, 0))
                .hasFieldOrPropertyWithValue("systemTimeEnd", LocalDateTime.of(9999, 12, 31, 0, 0, 0));

        QueryBuilderFactory.insert(Trade.class)
                .from(trade2)
                .execute(dataSource, entityManager);

        final List<Trade> results = new JdbcTemplate(dataSource).query("select id, valid_time_start, valid_time_end, system_time_start, system_time_end, volume, price, market_limit_flag, buy_sell_flag, stock from reporting.trade_data where id = '" + tradeId + "' order by system_time_start ASC", new BeanPropertyRowMapper<Trade>(Trade.class));
        assertThat(results).isNotNull().hasSize(2);

        assertThat(results.get(0))
                .hasFieldOrPropertyWithValue("systemTimeStart", LocalDateTime.of(2020, 1, 20, 3, 45, 0));
        assertThat(results.get(0).getSystemTimeEnd()).isBetween(now.minusSeconds(1), now.plusSeconds(1));

        assertThat(results.get(1))
                .hasFieldOrPropertyWithValue("systemTimeStart", LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                .hasFieldOrPropertyWithValue("systemTimeEnd", LocalDateTime.of(9999, 12, 31, 0, 0, 0));
    }

    @Test
    void insertFromObjectThrowsForOverlappingValidTime() throws OverlappingKeyException {
        final UUID tradeId = UUID.fromString("769fb864-f3b7-4ca5-965e-bcff80088197");
        final Trade trade1 = new Trade().setTradeKey(
                new BitemporalKey.Builder()
                        .setTradeId(tradeId)
                        .setValidTimeStart(LocalDate.of(2020, 1, 18))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                        .build())
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 20, 3, 45, 0))
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL");

        final Trade trade2 = new Trade().setTradeKey(
                new BitemporalKey.Builder()
                        .setTradeId(tradeId)
                        .setValidTimeStart(LocalDate.of(2020, 1, 19))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                        .build())
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL");

        QueryBuilderFactory.insert(Trade.class)
                .from(trade1)
                .execute(dataSource, entityManager);

        assertThat(assertThrows(OverlappingKeyException.class, () -> QueryBuilderFactory.insert(Trade.class)
                .from(trade2)
                .execute(dataSource, entityManager)
        )).hasMessage("overlapping valid time for id = '769fb864-f3b7-4ca5-965e-bcff80088197'");
    }
}

package kieranbrown.bitemp.database;

import io.vavr.collection.Stream;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import kieranbrown.bitemp.utils.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
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
    void insertFromObjectInsertsObject() throws InvalidPeriodException {
        final LocalDateTime now = LocalDateTime.now();
        final UUID tradeId = UUID.randomUUID();
        final BitemporalKey key = new BitemporalKey.Builder()
                .setTradeId(tradeId)
                .setValidTimeStart(LocalDate.of(2020, 1, 20))
                .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                .build();
        final Trade trade = new Trade().setBitemporalKey(key)
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL");

        QueryBuilderFactory.insert(Trade.class).from(trade).execute(entityManager);

        final Trade results = (Trade) entityManager.createNativeQuery("select * from reporting.trade_data where valid_time_start = ?1 and valid_time_end = ?2 and id = ?3", Trade.class)
                .setParameter(1, LocalDate.of(2020, 1, 20))
                .setParameter(2, LocalDate.of(2020, 1, 21))
                .setParameter(3, tradeId)
                .getResultList()
                .get(0);
        assertThat(
                results)
                .isNotNull()
                .usingRecursiveComparison()
                .ignoringFields("systemTimeStart", "systemTimeEnd")
                .isEqualTo(trade);

        assertThat(results.getSystemTimeStart()).isAfter(now);
        assertThat(results.getSystemTimeEnd()).isEqualToIgnoringNanos(Constants.MARIADB_END_SYSTEM_TIME);
    }

    @Test
    void canInsertMultipleObjectsAtOnce() throws InvalidPeriodException {
        final LocalDateTime now = LocalDateTime.now();
        final Trade trade1 = new Trade().setBitemporalKey(new BitemporalKey.Builder()
                .setTradeId(UUID.randomUUID())
                .setValidTimeStart(LocalDate.of(2020, 1, 20))
                .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                .build())
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL");

        final Trade trade2 = new Trade().setBitemporalKey(new BitemporalKey.Builder()
                .setTradeId(UUID.randomUUID())
                .setValidTimeStart(LocalDate.of(2020, 1, 20))
                .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                .build())
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL");

        QueryBuilderFactory.insert(Trade.class).fromAll(trade1, trade2).execute(entityManager);
        assertTradesAreEqual(trade1, trade2, now);

        QueryBuilderFactory.insert(Trade.class).fromAll(io.vavr.collection.List.of(trade1, trade2)).execute(entityManager);
        assertTradesAreEqual(trade1, trade2, now);

        QueryBuilderFactory.insert(Trade.class).fromAll(Stream.of(trade1, trade2)).execute(entityManager);
        assertTradesAreEqual(trade1, trade2, now);
    }

    @SuppressWarnings("unchecked")
    private void assertTradesAreEqual(final Trade trade1, final Trade trade2, final LocalDateTime now) {
        final List<Trade> arrayResults = entityManager.createNativeQuery("select * from reporting.trade_data where valid_time_start = ?1 and valid_time_end = ?2", Trade.class)
                .setParameter(1, LocalDate.of(2020, 1, 20))
                .setParameter(2, LocalDate.of(2020, 1, 21))
                .getResultList();
        assertThat(arrayResults).isNotNull().hasSize(2);
        assertThat(arrayResults.get(0)).usingRecursiveComparison().ignoringFields("systemTimeStart", "systemTimeEnd").isEqualTo(trade1);
        assertThat(arrayResults.get(0).getSystemTimeStart()).isAfter(now);
        assertThat(arrayResults.get(0).getSystemTimeEnd()).isEqualToIgnoringNanos(Constants.MARIADB_END_SYSTEM_TIME);

        assertThat(arrayResults.get(1)).usingRecursiveComparison().ignoringFields("systemTimeStart", "systemTimeEnd").isEqualTo(trade2);
        assertThat(arrayResults.get(1).getSystemTimeStart()).isAfter(now);
        assertThat(arrayResults.get(1).getSystemTimeEnd()).isEqualToIgnoringNanos(Constants.MARIADB_END_SYSTEM_TIME);

        entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
    }

    @Test
    void insertThrowsForUnbalancedValidTime() {
        final UUID tradeId = UUID.randomUUID();
        final InsertQueryBuilder<Trade> queryBuilder = QueryBuilderFactory.insert(Trade.class).from(new Trade().setBitemporalKey(new BitemporalKey.Builder()
                .setTradeId(tradeId)
                .setValidTimeStart(LocalDate.of(2020, 1, 21))
                .setValidTimeEnd(LocalDate.of(2020, 1, 20))
                .build())
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 20, 3, 45, 0))
                .setSystemTimeEnd(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL"));

        assertThat(assertThrows(InvalidPeriodException.class, () -> queryBuilder.execute(entityManager)))
                .hasMessage("Valid Time End is before Start for ID = '" + tradeId.toString() + "'");
    }
}

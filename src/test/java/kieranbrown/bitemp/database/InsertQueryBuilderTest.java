package kieranbrown.bitemp.database;

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

        QueryBuilderFactory.insert(Trade.class).from(trade).execute(dataSource);

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
    void insertFromObjectUpdatesExistingSystemTime() {
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
                        .setValidTimeEnd(LocalDate.of(2020, 1, 22))
                        .build())
                .setSystemTimeStart(LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL");

        QueryBuilderFactory.insert(Trade.class)
                .from(trade1)
                .execute(dataSource);

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
                .execute(dataSource);

        final List<Trade> results = new JdbcTemplate(dataSource).query("select id, valid_time_start, valid_time_end, system_time_start, system_time_end, volume, price, market_limit_flag, buy_sell_flag, stock from reporting.trade_data where id = '" + tradeId + "' order by system_time_start ASC", new BeanPropertyRowMapper<Trade>(Trade.class));
        assertThat(results).isNotNull().hasSize(2);

        assertThat(results.get(0))
                .hasFieldOrPropertyWithValue("systemTimeStart", LocalDateTime.of(2020, 1, 20, 3, 45, 0));
        assertThat(results.get(0).getSystemTimeEnd()).isBetween(now.minusSeconds(1), now.plusSeconds(1));

        assertThat(results.get(1))
                .hasFieldOrPropertyWithValue("systemTimeStart", LocalDateTime.of(2020, 1, 21, 3, 45, 0))
                .hasFieldOrPropertyWithValue("systemTimeEnd", LocalDateTime.of(9999, 12, 31, 0, 0, 0));
    }
}

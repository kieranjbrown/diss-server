package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig
@DataJpaTest
class BitemporalRepositoryTest {

    @Autowired
    private TradeRepository repository;

    @Test
    void canPersistBitemporalModel() {
        final Trade trade = new Trade();
        final BitemporalKey key = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
        trade.setTradeKey(key);
        trade.setStock("GOOGL");
        trade.setBuySellFlag('B');
        trade.setMarketLimitFlag('M');
        trade.setPrice(new BigDecimal("140.171"));
        trade.setVolume(200);
        trade.setSystemTimeStart(new Date(2009, 10, 10, 10, 10, 37));
        trade.setSystemTimeEnd(new Date(2009, 10, 10, 12, 10, 37));
        trade.setValidTimeStart(new Date(2009, 10, 10, 9, 30, 0));
        trade.setValidTimeEnd(new Date(2009, 10, 10, 12, 8, 30));

        repository.save(trade);

        final Trade retrievedTrade = repository.getOne(key);
        assertThat(retrievedTrade).isNotNull()
                .hasFieldOrPropertyWithValue("stock", "GOOGL")
                .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                .hasFieldOrPropertyWithValue("price", new BigDecimal("140.171"))
                .hasFieldOrPropertyWithValue("volume", 200)
                .hasFieldOrPropertyWithValue("tradeKey", key);
    }
}

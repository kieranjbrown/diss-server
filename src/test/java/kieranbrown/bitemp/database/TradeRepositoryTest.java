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
class TradeRepositoryTest {

    @Autowired
    private TradeRepository repository;

    @Test
    void canPersistBitemporalModel() {
        final Trade trade = new Trade();
        final BitemporalKey key = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
        trade.setTradeKey(key).
                setStock("GOOGL").
                setBuySellFlag('B').
                setMarketLimitFlag('M').
                setPrice(new BigDecimal("140.171")).
                setVolume(200).
                setSystemTimeStart(new Date(2009, 10, 10, 10, 10, 37)).
                setValidTimeStart(new Date(2009, 10, 10, 9, 30, 0));

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

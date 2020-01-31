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
        trade.setTradeKey(key)
                .setStock("GOOGL")
                .setBuySellFlag('B')
                .setMarketLimitFlag('M')
                .setPrice(new BigDecimal("140.171"))
                .setVolume(200)
                .setValidTimeStart(new Date(2009, 10, 10, 9, 30, 0));

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

    @Test
    void canRetrieveMostRecentRowForGivenTradeUsingUUID() {
        final Trade trade1 = new Trade();
        final Trade trade2 = new Trade();
        final UUID tradeId = UUID.randomUUID();
        final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(tradeId).setVersion(200).build();
        final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(tradeId).setVersion(201).build();
        trade1.setTradeKey(key1)
                .setStock("GOOGL")
                .setBuySellFlag('B')
                .setMarketLimitFlag('M')
                .setPrice(new BigDecimal("140.171"))
                .setVolume(200)
                .setSystemTimeEnd(new Date(2009, 10, 10, 10, 30, 0))
                .setValidTimeStart(new Date(2009, 10, 10, 9, 30, 0))
                .setValidTimeEnd(new Date(2009, 10, 10, 29, 57, 0));

        trade2.setTradeKey(key2)
                .setStock("GOOGL")
                .setBuySellFlag('B')
                .setMarketLimitFlag('M')
                .setPrice(new BigDecimal("142.120"))
                .setVolume(190)
                .setSystemTimeStart(new Date(2009, 10, 10, 10, 30, 0))
                .setValidTimeStart(new Date(2009, 10, 10, 29, 57, 0));

        repository.save(trade1);
        repository.save(trade2);
        System.out.println();
        final Trade retrievedTrade = repository.findByTradeId(tradeId);
        assertThat(retrievedTrade).isNotNull()
                .hasFieldOrPropertyWithValue("stock", "GOOGL")
                .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                .hasFieldOrPropertyWithValue("price", new BigDecimal("142.120"))
                .hasFieldOrPropertyWithValue("volume", 190)
                .hasFieldOrPropertyWithValue("tradeKey", key2)
                .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2009, 10, 10, 10, 30, 0))
                .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(9999, 12, 31, 0, 0, 0))
                .hasFieldOrPropertyWithValue("validTimeStart", new Date(2009, 10, 10, 29, 57, 0))
                .hasFieldOrPropertyWithValue("validTimeEnd", new Date(9999, 12, 31, 0, 0, 0));
    }
}

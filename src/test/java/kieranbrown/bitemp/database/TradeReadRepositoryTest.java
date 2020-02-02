package kieranbrown.bitemp.database;

import com.google.common.collect.ImmutableList;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TradeReadRepositoryTest {

    @SpringJUnitConfig
    @DataJpaTest
    @Nested
    class generic {
        @Autowired
        private TradeReadRepository repository;

        @Test
        void canPersistBitemporalModel() {
            final Trade trade = new Trade();
            final Date now = new Date();
            final BitemporalKey key = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            trade.setTradeKey(key)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("140.171"))
                    .setVolume(200)
                    .setValidTimeStart(LocalDate.of(2009, 10, 10));

            repository.save(trade);

            final Trade retrievedTrade = repository.getOne(key);
            assertThat(retrievedTrade).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "GOOGL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("140.171"))
                    .hasFieldOrPropertyWithValue("volume", 200)
                    .hasFieldOrPropertyWithValue("tradeKey", key)
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(9999, 12, 31, 0, 0, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2009, 10, 10))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(9999, 12, 31));

            assertThat(retrievedTrade.getSystemTimeStart()).isAfter(now);
        }

        @Test
        void canRetrieveAllByIdInAscOrder() {
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
                    .setSystemTimeStart(new Date(2009, 10, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2009, 10, 10, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2009, 10, 10))
                    .setValidTimeEnd(LocalDate.of(2009, 10, 10));

            trade2.setTradeKey(key2)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("142.120"))
                    .setVolume(190)
                    .setSystemTimeStart(new Date(2009, 10, 10, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2009, 10, 10));

            repository.save(trade1);
            repository.save(trade2);
            System.out.println();
            final List<Trade> retrievedTrades = repository.findAllById(tradeId);

            assertThat(retrievedTrades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "GOOGL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("140.171"))
                    .hasFieldOrPropertyWithValue("volume", 200)
                    .hasFieldOrPropertyWithValue("tradeKey", key1)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2009, 10, 10, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2009, 10, 10, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2009, 10, 10))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2009, 10, 10));

            assertThat(retrievedTrades.get(1)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "GOOGL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("142.120"))
                    .hasFieldOrPropertyWithValue("volume", 190)
                    .hasFieldOrPropertyWithValue("tradeKey", key2)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2009, 10, 10, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(9999, 12, 31, 0, 0, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2009, 10, 10))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(9999, 12, 31));
        }
    }

    @SpringJUnitConfig
    @DataJpaTest
    @Nested
    class SystemTimeTests {
        @Autowired
        private TradeReadRepository repository;

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
                    .setValidTimeStart(LocalDate.of(2009, 10, 10))
                    .setValidTimeEnd(LocalDate.of(2009, 10, 10));

            trade2.setTradeKey(key2)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("142.120"))
                    .setVolume(190)
                    .setSystemTimeStart(new Date(2009, 10, 10, 10, 30, 0))
                    .setValidTimeStart(LocalDate.of(2009, 10, 10));

            repository.save(trade1);
            repository.save(trade2);
            System.out.println();
            final Trade retrievedTrade = repository.findMostRecentBySystemTime(tradeId);
            assertThat(retrievedTrade).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "GOOGL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("142.120"))
                    .hasFieldOrPropertyWithValue("volume", 190)
                    .hasFieldOrPropertyWithValue("tradeKey", key2)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2009, 10, 10, 10, 30, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(9999, 12, 31, 0, 0, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2009, 10, 10))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(9999, 12, 31));
        }

        @Test
        void canRetrieveTradesBetweenSystemTimeRange() {
            final Date startRange = new Date(2020, 1, 10, 0, 0, 0);
            final Date endRange = new Date(2020, 1, 20, 0, 0, 0);
            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade2.setTradeKey(key2)
                    .setStock("AAPl")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 13))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 19));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3));

            final List<Trade> trades = repository.findAllBetweenSystemTimeRange(startRange, endRange);
            assertThat(trades).isNotNull().hasSize(1);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "GOOGL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("100.127"))
                    .hasFieldOrPropertyWithValue("volume", 200)
                    .hasFieldOrPropertyWithValue("tradeKey", key1)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 10, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 15, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 10))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 15));
        }

        @Test
        void canRetrieveTradesAsOfSystemTime() {
            final Date systemTime = new Date(2020, 1, 15, 0, 0, 0);
            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final Trade trade4 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key4 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(202).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 15, 3, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 10, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade2.setTradeKey(key2)
                    .setStock("AAPL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 13))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 19));

            trade4.setTradeKey(key4)
                    .setStock("EBAY")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("178.345"))
                    .setVolume(110)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 12, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 13))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 19));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3));

            final List<Trade> trades = repository.findAllAsOfSystemTime(systemTime);
            assertThat(trades).isNotNull().hasSize(2);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "AAPL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("189.213"))
                    .hasFieldOrPropertyWithValue("volume", 195)
                    .hasFieldOrPropertyWithValue("tradeKey", key2)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 9, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 15, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 9))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 15));

            assertThat(trades.get(1)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "MSFT")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'S')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("78.345"))
                    .hasFieldOrPropertyWithValue("volume", 199)
                    .hasFieldOrPropertyWithValue("tradeKey", key3)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 10, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 21, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 13))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 19));
        }

        @Test
        void canRetrieveTradesFromSystemTimeRange() {
            final Date startTime = new Date(2020, 1, 10, 0, 0, 0);
            final Date endTime = new Date(2020, 1, 20, 0, 0, 0);
            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade2.setTradeKey(key2)
                    .setStock("AAPl")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 20, 0, 0, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 13))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 19));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3));

            final List<Trade> trades = repository.findAllFromSystemTimeRange(startTime, endTime);
            assertThat(trades).isNotNull().hasSize(1);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "GOOGL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("100.127"))
                    .hasFieldOrPropertyWithValue("volume", 200)
                    .hasFieldOrPropertyWithValue("tradeKey", key1)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 10, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 15, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 10))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 15));
        }
    }

    @SpringJUnitConfig
    @DataJpaTest
    @Nested
    class ValidTimeTests {
        @Autowired
        private TradeReadRepository repository;

        @Test
        void canRetrievesTradesContainingValidTime() {
            final LocalDate validTime = LocalDate.of(2020, 1, 15);
            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final Trade trade4 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key4 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(202).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 15, 3, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 10, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade2.setTradeKey(key2)
                    .setStock("AAPL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 9))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 15))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 19));

            trade4.setTradeKey(key4)
                    .setStock("EBAY")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("178.345"))
                    .setVolume(110)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 12, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 13))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 19));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3, trade4));

            final List<Trade> trades = repository.findAllContainsValidTime(validTime);
            assertThat(trades).isNotNull().hasSize(2);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "MSFT")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'S')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("78.345"))
                    .hasFieldOrPropertyWithValue("volume", 199)
                    .hasFieldOrPropertyWithValue("tradeKey", key3)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 10, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 21, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 15))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 19));

            assertThat(trades.get(1)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "EBAY")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("178.345"))
                    .hasFieldOrPropertyWithValue("volume", 110)
                    .hasFieldOrPropertyWithValue("tradeKey", key4)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 10, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 12, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 13))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 19));

        }

        @Test
        void canRetrieveTradesOverlappingValidTimeRange() {
            final LocalDate startDate = LocalDate.of(2020, 1, 15);
            final LocalDate endDate = LocalDate.of(2020, 1, 20);
            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 16))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 21));

            trade2.setTradeKey(key2)
                    .setStock("AAPL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 14))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 21));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 14))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 19));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3));

            final List<Trade> trades = repository.findAllOverlappingValidTimeRange(startDate, endDate);
            assertThat(trades).isNotNull().hasSize(1);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "AAPL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("189.213"))
                    .hasFieldOrPropertyWithValue("volume", 195)
                    .hasFieldOrPropertyWithValue("tradeKey", key2)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 9, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 15, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 14))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 21));
        }

        @Test
        void canRetrieveTradesWhereValidTimeIsEqual() {
            final LocalDate startDate = LocalDate.of(2020, 1, 12);
            final LocalDate endDate = LocalDate.of(2020, 1, 15);

            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 12))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade2.setTradeKey(key2)
                    .setStock("AAPL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 12))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 16));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 14))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3));

            final List<Trade> trades = repository.findAllWhereValidTimeIsEqual(startDate, endDate);
            assertThat(trades).isNotNull().hasSize(1);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "GOOGL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("100.127"))
                    .hasFieldOrPropertyWithValue("volume", 200)
                    .hasFieldOrPropertyWithValue("tradeKey", key1)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 10, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 15, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 12))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 15));
        }

        @Test
        void canRetrieveTradesWhereValidTimeIsPreceded() {
            final LocalDate startDate = LocalDate.of(2020, 1, 11);
            final LocalDate endDate = LocalDate.of(2020, 1, 12);

            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final Trade trade4 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key4 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(210).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 12))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade2.setTradeKey(key2)
                    .setStock("AAPL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 11));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 12));

            trade4.setTradeKey(key4)
                    .setStock("EBAY")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("68.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 11))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3, trade4));

            final List<Trade> trades = repository.findAllWhereValidTimeIsPreceded(startDate, endDate);
            assertThat(trades).isNotNull().hasSize(2);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "AAPL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("189.213"))
                    .hasFieldOrPropertyWithValue("volume", 195)
                    .hasFieldOrPropertyWithValue("tradeKey", key2)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 9, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 15, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 10))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 11));

            assertThat(trades.get(1)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "MSFT")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'S')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("78.345"))
                    .hasFieldOrPropertyWithValue("volume", 199)
                    .hasFieldOrPropertyWithValue("tradeKey", key3)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 15, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 21, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 10))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 12));
        }

        @Test
        void canRetrieveTradesWhereValidTimeIsImmediatelyPreceded() {
            final LocalDate startDate = LocalDate.of(2020, 1, 11);
            final LocalDate endDate = LocalDate.of(2020, 1, 12);

            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final Trade trade4 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key4 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(210).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 12))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade2.setTradeKey(key2)
                    .setStock("AAPL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 11));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 12));

            trade4.setTradeKey(key4)
                    .setStock("EBAY")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("68.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 11))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3, trade4));

            final List<Trade> trades = repository.findAllWhereValidTimeIsImmediatelyPreceded(startDate, endDate);
            assertThat(trades).isNotNull().hasSize(1);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "MSFT")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'S')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("78.345"))
                    .hasFieldOrPropertyWithValue("volume", 199)
                    .hasFieldOrPropertyWithValue("tradeKey", key3)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 15, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 21, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 10))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 12));
        }

        @Test
        void canRetrieveTradesWhereValidTimeIsSucceeded() {
            final LocalDate endDate = LocalDate.of(2020, 1, 12);

            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final Trade trade4 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key4 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(210).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 12))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade2.setTradeKey(key2)
                    .setStock("AAPL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 11));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 12));

            trade4.setTradeKey(key4)
                    .setStock("EBAY")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("68.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 13))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3, trade4));

            final List<Trade> trades = repository.findAllWhereValidTimeIsSucceeded(endDate);
            assertThat(trades).isNotNull().hasSize(2);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "GOOGL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("100.127"))
                    .hasFieldOrPropertyWithValue("volume", 200)
                    .hasFieldOrPropertyWithValue("tradeKey", key1)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 10, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 15, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 12))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 15));

            assertThat(trades.get(1)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "EBAY")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'S')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("68.345"))
                    .hasFieldOrPropertyWithValue("volume", 199)
                    .hasFieldOrPropertyWithValue("tradeKey", key4)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 15, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 21, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 13))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 15));
        }

        @Test
        void canRetrieveTradesWhereValidTimeIsImmediatelySucceeded() {
            final LocalDate endDate = LocalDate.of(2020, 1, 12);

            final Trade trade1 = new Trade();
            final Trade trade2 = new Trade();
            final Trade trade3 = new Trade();
            final Trade trade4 = new Trade();
            final BitemporalKey key1 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key2 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(201).build();
            final BitemporalKey key3 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
            final BitemporalKey key4 = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(210).build();

            trade1.setTradeKey(key1)
                    .setStock("GOOGL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("100.127"))
                    .setVolume(200)
                    .setSystemTimeStart(new Date(2020, 1, 10, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 12))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            trade2.setTradeKey(key2)
                    .setStock("AAPL")
                    .setBuySellFlag('B')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("189.213"))
                    .setVolume(195)
                    .setSystemTimeStart(new Date(2020, 1, 9, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 15, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 11));

            trade3.setTradeKey(key3)
                    .setStock("MSFT")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("78.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 10))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 12));

            trade4.setTradeKey(key4)
                    .setStock("EBAY")
                    .setBuySellFlag('S')
                    .setMarketLimitFlag('M')
                    .setPrice(new BigDecimal("68.345"))
                    .setVolume(199)
                    .setSystemTimeStart(new Date(2020, 1, 15, 10, 0, 0))
                    .setSystemTimeEnd(new Date(2020, 1, 21, 3, 30, 0))
                    .setValidTimeStart(LocalDate.of(2020, 1, 13))
                    .setValidTimeEnd(LocalDate.of(2020, 1, 15));

            repository.saveAll(ImmutableList.of(trade1, trade2, trade3, trade4));

            final List<Trade> trades = repository.findAllWhereValidTimeIsImmediatelySucceeded(endDate);
            assertThat(trades).isNotNull().hasSize(1);

            assertThat(trades.get(0)).isNotNull()
                    .hasFieldOrPropertyWithValue("stock", "GOOGL")
                    .hasFieldOrPropertyWithValue("marketLimitFlag", 'M')
                    .hasFieldOrPropertyWithValue("buySellFlag", 'B')
                    .hasFieldOrPropertyWithValue("price", new BigDecimal("100.127"))
                    .hasFieldOrPropertyWithValue("volume", 200)
                    .hasFieldOrPropertyWithValue("tradeKey", key1)
                    .hasFieldOrPropertyWithValue("systemTimeStart", new Date(2020, 1, 10, 10, 0, 0))
                    .hasFieldOrPropertyWithValue("systemTimeEnd", new Date(2020, 1, 15, 3, 30, 0))
                    .hasFieldOrPropertyWithValue("validTimeStart", LocalDate.of(2020, 1, 12))
                    .hasFieldOrPropertyWithValue("validTimeEnd", LocalDate.of(2020, 1, 15));
        }
    }

    @SpringJUnitConfig
    @DataJpaTest
    @Nested
    class BitemporalTests {
        @Autowired
        private TradeReadRepository repository;
    }
}
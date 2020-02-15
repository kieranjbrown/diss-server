package kieranbrown.bitemp.database;

import com.google.common.collect.ImmutableList;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

@SpringJUnitConfig
@DataJpaTest
class Demo {
    @Autowired
    private TradeWriteRepository repository;

    @PersistenceContext
    private EntityManager entityManager;

    @Test
    void retrievingDataWithColumnFilters() {
        repository.saveAll(ImmutableList.of(
                new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                        .setValidTimeStart(LocalDate.of(2020, 1, 20))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                        .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                        .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                        .setVolume(200)
                        .setPrice(new BigDecimal("123.45"))
                        .setMarketLimitFlag('M')
                        .setBuySellFlag('B')
                        .setStock("AMZN"),
                new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                        .setValidTimeStart(LocalDate.of(2020, 1, 19))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                        .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                        .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                        .setVolume(200)
                        .setPrice(new BigDecimal("123.45"))
                        .setMarketLimitFlag('M')
                        .setBuySellFlag('B')
                        .setStock("EBAY"),
                new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                        .setValidTimeStart(LocalDate.of(2020, 1, 21))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                        .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                        .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                        .setVolume(200)
                        .setPrice(new BigDecimal("456.12"))
                        .setMarketLimitFlag('M')
                        .setBuySellFlag('B')
                        .setStock("GOOGL"),
                new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                        .setValidTimeStart(LocalDate.of(2020, 1, 10))
                        .setValidTimeEnd(LocalDate.of(2020, 1, 11))
                        .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                        .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                        .setVolume(150)
                        .setPrice(new BigDecimal("123.45"))
                        .setMarketLimitFlag('M')
                        .setBuySellFlag('B')
                        .setStock("GOOGL")
        ));

        System.out.println("retrieve all where stock is google");
        QueryBuilder.select(Trade.class)
                .allFields()
                .where("stock", QueryEquality.EQUALS, "GOOGL")
                .execute(entityManager)
                .getResults()
                .forEach(System.out::println);
    }

    @Test
    void retrievingDataWithTimeFilters() {
        repository.saveAll(ImmutableList.of(
                new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                        .setValidTimeStart(LocalDate.of(2020, 2, 20))
                        .setValidTimeEnd(LocalDate.of(2020, 2, 21))
                        .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                        .setSystemTimeEnd(new Date(120, 0, 11, 3, 45, 0))
                        .setVolume(200)
                        .setPrice(new BigDecimal("123.45"))
                        .setMarketLimitFlag('M')
                        .setBuySellFlag('B')
                        .setStock("AMZN"),
                new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                        .setValidTimeStart(LocalDate.of(2020, 2, 9))
                        .setValidTimeEnd(LocalDate.of(2020, 2, 21))
                        .setSystemTimeStart(new Date(120, 0, 9, 3, 45, 0))
                        .setSystemTimeEnd(new Date(120, 0, 13, 3, 45, 0))
                        .setVolume(200)
                        .setPrice(new BigDecimal("123.45"))
                        .setMarketLimitFlag('M')
                        .setBuySellFlag('B')
                        .setStock("EBAY"),
                new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(4).build())
                        .setValidTimeStart(LocalDate.of(2020, 1, 21))
                        .setValidTimeEnd(LocalDate.of(2020, 2, 5))
                        .setSystemTimeStart(new Date(120, 0, 10, 0, 0, 0))
                        .setSystemTimeEnd(new Date(120, 0, 10, 0, 0, 0))
                        .setVolume(200)
                        .setPrice(new BigDecimal("456.12"))
                        .setMarketLimitFlag('M')
                        .setBuySellFlag('B')
                        .setStock("MSFT"),
                new Trade().setTradeKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(3).build())
                        .setValidTimeStart(LocalDate.of(2020, 1, 10))
                        .setValidTimeEnd(LocalDate.of(2020, 2, 10))
                        .setSystemTimeStart(new Date(120, 0, 19, 3, 45, 0))
                        .setSystemTimeEnd(new Date(120, 0, 21, 3, 45, 0))
                        .setVolume(150)
                        .setPrice(new BigDecimal("123.45"))
                        .setMarketLimitFlag('M')
                        .setBuySellFlag('B')
                        .setStock("GOOGL")
        ));

        System.out.println("retrieve all where valid time precedes 20th February");
        QueryBuilder.select(Trade.class)
                .allFields()
                .validTimePrecedes(LocalDate.of(2020, 2, 10))
                .execute(entityManager)
                .getResults()
                .forEach(System.out::println);
    }
}

package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig
@DataJpaTest
class QueryBuilderTest {

    @Autowired
    private TradeWriteRepository repository;

    private static final BitemporalKey KEY = new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setVersion(200).build();
    private static final QueryBuilder SELECT_ONE_TRADE = QueryBuilder.selectOne(Trade.class);

    @Test
    void canCreateQueryForSingleResult() {
        assertThat(SELECT_ONE_TRADE).isNotNull()
                .hasFieldOrPropertyWithValue("query", QueryBuilder.selectOne(Trade.class))
                .hasFieldOrPropertyWithValue("queryClass", Trade.class);
    }

    @Test
    void canCreateQueryForMultipleResults() {
        assertThat(QueryBuilder.selectMultiple(Trade.class)).isNotNull()
                .hasFieldOrPropertyWithValue("query", new Query(QueryType.SELECT, Trade.class))
                .hasFieldOrPropertyWithValue("queryClass", Trade.class);
    }

    @Test
    void allFieldsSetsAllFieldsFromTheSourceObject() {
        final Map<String, Object> fields = HashMap.ofEntries(
                new Tuple2<>("id", null),
                new Tuple2<>("version", null),
                new Tuple2<>("valid_time_start", null),
                new Tuple2<>("valid_time_end", null),
                new Tuple2<>("system_time_start", null),
                new Tuple2<>("system_time_end", null),
                new Tuple2<>("stock", null),
                new Tuple2<>("price", null),
                new Tuple2<>("volume", null),
                new Tuple2<>("buy_sell_flag", null),
                new Tuple2<>("market_limit_flag", null)
        );

        System.out.println("test fields");
        fields.keySet().forEach(System.out::println);

        final Query query = new Query(QueryType.SELECT_DISTINCT, Trade.class);
        query.setFields(fields);

        final QueryBuilder queryBuilder = QueryBuilder.selectOne(Trade.class).allFields();

        assertThat(queryBuilder).isNotNull()
                .extracting("query")
                .extracting("fields")
                .isEqualTo(fields);
    }

    @Test
    void canEvaluateSingleSelectQueryWithAllFieldsAndReturnResult() {
        final Trade trade = new Trade().setTradeKey(KEY)
                .setValidTimeStart(LocalDate.of(2020, 1, 20))
                .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                .setSystemTimeStart(new Date(2020, 1, 20, 3, 45, 0))
                .setSystemTimeStart(new Date(2020, 1, 21, 3, 45, 0))
                .setVolume(200)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("GOOGL");

        repository.save(trade);

        QueryBuilder.selectOne(Trade.class).allFields();
    }
}

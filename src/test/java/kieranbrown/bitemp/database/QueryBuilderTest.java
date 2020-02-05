package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class QueryBuilderTest {

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
                .hasFieldOrPropertyWithValue("query", new Query(QueryType.SELECT_MANY, Trade.class))
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

        final Query query = new Query(QueryType.SELECT_ONE, Trade.class);
        query.setFields(fields);

        final QueryBuilder queryBuilder = QueryBuilder.selectOne(Trade.class).allFields();

        assertThat(queryBuilder).isNotNull()
                .extracting("query")
                .extracting("fields")
                .isEqualTo(fields);
    }
}

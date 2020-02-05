package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryTest {
    @Test
    void constructorThrowsForInvalidInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new Query<>(null, Trade.class)))
                .hasMessage("queryType cannot be null");

        assertThat(assertThrows(NullPointerException.class, () -> new Query<>(QueryType.SELECT_DISTINCT, null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void setFieldsThrowsForNullInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new Query<>(QueryType.SELECT_DISTINCT, Trade.class).setFields(null)))
                .hasMessage("fields cannot be null");
    }

    @Test
    void setFieldsSetsTheFieldsAndValues() {
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

        final Query query = new Query<>(QueryType.SELECT_DISTINCT, Trade.class);
        query.setFields(fields);

        assertThat(query).hasFieldOrPropertyWithValue("fields", fields);
    }

    @Test
    void buildReturnsValidSelectQueryForSetFields() {
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

        final Query query = new Query<>(QueryType.SELECT, Trade.class);
        query.setFields(fields);

        final String sql = query.build();
        assertThat(sql).isEqualTo(
                "SELECT system_time_start, buy_sell_flag, price, valid_time_end, valid_time_start, market_limit_flag, " +
                        "stock, version, system_time_end, volume, id from reporting.trade_data");
    }
}

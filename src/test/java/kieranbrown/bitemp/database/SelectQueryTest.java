package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SelectQueryTest {
    @Test
    void constructorThrowsForInvalidInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new SelectQuery<>(null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void setFieldsThrowsForNullInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new SelectQuery<>(Trade.class).setFields(null)))
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

        final SelectQuery selectQuery = new SelectQuery<>(Trade.class);
        selectQuery.setFields(fields);

        assertThat(selectQuery).hasFieldOrPropertyWithValue("fields", fields);
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

        final SelectQuery selectQuery = new SelectQuery<>(Trade.class);
        selectQuery.setFields(fields);

        final String sql = selectQuery.build();
        assertThat(sql).isEqualTo(
                "SELECT system_time_start, buy_sell_flag, price, valid_time_end, valid_time_start, market_limit_flag, " +
                        "stock, version, system_time_end, volume, id from reporting.trade_data");
    }

    @Test
    void settingLimitAffectsQuery() {
        final Map<String, Object> fields = HashMap.ofEntries(
                new Tuple2<>("id", null),
                new Tuple2<>("version", null)
        );

        final SelectQuery selectQuery = new SelectQuery<>(Trade.class);
        selectQuery.setFields(fields);
        selectQuery.setLimit(3);

        final String sql = selectQuery.build();
        assertThat(sql).isEqualTo(
                "SELECT version, id from reporting.trade_data limit 3");
    }

    @Test
    void notSettingLimitOrSettingNoLimitDoesNotAffectQuery() {
        final Map<String, Object> fields = HashMap.ofEntries(
                new Tuple2<>("id", null),
                new Tuple2<>("version", null)
        );

        final SelectQuery selectQuery = new SelectQuery<>(Trade.class);
        selectQuery.setFields(fields);

        assertThat(selectQuery.build()).isEqualTo(
                "SELECT version, id from reporting.trade_data");

        selectQuery.setLimit(-1);
        assertThat(selectQuery.build()).isEqualTo(
                "SELECT version, id from reporting.trade_data");
    }

    @Test
    void notSettingFieldsSelectsAllFields() {
        final SelectQuery<Trade> selectQuery = new SelectQuery<>(Trade.class);
        assertThat(selectQuery.build()).isEqualTo("SELECT * from reporting.trade_data");
    }

    @Test
    void settingFilterAddsToQuery() {
        final SelectQuery<Trade> selectQuery = new SelectQuery<>(Trade.class);
        selectQuery.setFilters(List.of(new SingleQueryFilter(new Tuple3<>("id", QueryEquality.EQUALS, 3))));
        assertThat(selectQuery.build()).isEqualTo("SELECT * from reporting.trade_data where id = 3");
    }

    @Test
    void canBuildQueryWithOrFilter() {
        final SelectQuery selectQuery = new SelectQuery<>(Trade.class);
        selectQuery.setFilters(List.of(
                new SingleQueryFilter(new Tuple3<>("id", QueryEquality.EQUALS, 3)),
                new OrQueryFilter(
                        new Tuple3<>("version", QueryEquality.GREATER_THAN_EQUAL_TO, 10),
                        new Tuple3<>("version", QueryEquality.LESS_THAN_EQUAL_TO, -10)
                )
        ));

        assertThat(selectQuery.build()).isEqualTo("SELECT * from reporting.trade_data where id = 3 and (version >= 10 OR version <= -10)");
    }
}

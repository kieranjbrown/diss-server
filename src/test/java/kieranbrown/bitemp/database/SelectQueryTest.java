package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SelectQueryTest {
    @Test
    void constructorThrowsForInvalidInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new SelectQuery<>(null, Trade.class)))
                .hasMessage("queryType cannot be null");

        assertThat(assertThrows(NullPointerException.class, () -> new SelectQuery<>(QueryType.SELECT, null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void setFieldsThrowsForNullInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new SelectQuery<>(QueryType.SELECT, Trade.class).setFields(null)))
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

        final SelectQuery selectQuery = new SelectQuery<>(QueryType.SELECT, Trade.class);
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

        final SelectQuery selectQuery = new SelectQuery<>(QueryType.SELECT, Trade.class);
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

        final SelectQuery selectQuery = new SelectQuery<>(QueryType.SELECT, Trade.class);
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

        final SelectQuery selectQuery = new SelectQuery<>(QueryType.SELECT, Trade.class);
        selectQuery.setFields(fields);

        assertThat(selectQuery.build()).isEqualTo(
                "SELECT version, id from reporting.trade_data");

        selectQuery.setLimit(-1);
        assertThat(selectQuery.build()).isEqualTo(
                "SELECT version, id from reporting.trade_data");
    }

    @Test
    void notSettingFieldsSelectsAllFields() {
        final SelectQuery<Trade> selectQuery = new SelectQuery<>(QueryType.SELECT, Trade.class);
        assertThat(selectQuery.build()).isEqualTo("SELECT * from reporting.trade_data");
    }

    @Test
    void settingFilterAddsToQuery() {
        final SelectQuery<Trade> selectQuery = new SelectQuery<>(QueryType.SELECT, Trade.class);
        selectQuery.setFilters(List.of(new SingleQueryFilter(new Tuple3<>("id", QueryEquality.EQUALS, 3))));
        assertThat(selectQuery.build()).isEqualTo("SELECT * from reporting.trade_data where id = 3");
    }

    @Test
    void canBuildQueryWithOrFilter() {
        final SelectQuery selectQuery = new SelectQuery<>(QueryType.SELECT, Trade.class);
        selectQuery.setFilters(List.of(
                new SingleQueryFilter(new Tuple3<>("id", QueryEquality.EQUALS, 3)),
                new OrQueryFilter(
                        new Tuple3<>("version", QueryEquality.GREATER_THAN_EQUAL_TO, 10),
                        new Tuple3<>("version", QueryEquality.LESS_THAN_EQUAL_TO, -10)
                )
        ));

        assertThat(selectQuery.build()).isEqualTo("SELECT * from reporting.trade_data where id = 3 and (version >= 10 OR version <= -10)");
    }

    @Test
    void canBuildInsertQuery() {
        final UUID tradeId = UUID.randomUUID();
        final SelectQuery<Trade> tradeSelectQuery = new SelectQuery<>(QueryType.INSERT, Trade.class);
        tradeSelectQuery.setFields(HashMap.ofEntries(
                new Tuple2<>("id", tradeId),
                new Tuple2<>("stock", "GOOGL"),
                new Tuple2<>("buy_sell_flag", 'B'),
                new Tuple2<>("market_limit_flag", 'M'),
                new Tuple2<>("price", new BigDecimal("123.45")),
                new Tuple2<>("volume", 200),
                new Tuple2<>("system_time_start", LocalDateTime.of(2020, 1, 20, 3, 45, 0)),
                new Tuple2<>("system_time_end", LocalDateTime.of(2020, 1, 21, 3, 45, 0)),
                new Tuple2<>("valid_time_start", LocalDate.of(2020, 1, 20)),
                new Tuple2<>("valid_time_end", LocalDate.of(2020, 1, 21))
        ));

        assertThat(tradeSelectQuery.build()).isNotNull().isEqualTo("INSERT INTO reporting.trade_data " +
                "(system_time_start, buy_sell_flag, price, valid_time_end, valid_time_start, market_limit_flag, stock, system_time_end, volume, id) VALUES " +
                "('2020-01-20 03:45:00.000000', 'B', 123.45, '2020-01-21', '2020-01-20', 'M', 'GOOGL', '2020-01-21 03:45:00.000000', 200, '" + tradeId + "')");
    }
}

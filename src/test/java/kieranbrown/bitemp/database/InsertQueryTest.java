package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InsertQueryTest {

    @Test
    void constructorThrowsForInvalidInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new InsertQuery<>(null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void addFieldsStoresValues() {
        final List<List<Tuple2<String, Object>>> fields = List.of(
                List.of(
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
                )
        );

        final InsertQuery query = new InsertQuery<>(Trade.class);
        query.addFields(fields);

        assertThat(query).hasFieldOrPropertyWithValue("fields", fields);
    }

    @Test
    void buildWorksForSingleInput() {
        final UUID id = UUID.fromString("769fb864-f3b7-4ca5-965e-bcff80088197");
        final List<List<Tuple2<String, Object>>> fields = List.of(
                List.of(
                        new Tuple2<>("id", id),
                        new Tuple2<>("valid_time_start", LocalDate.of(2020, 1, 20)),
                        new Tuple2<>("valid_time_end", LocalDate.of(2020, 1, 21)),
                        new Tuple2<>("system_time_start", LocalDateTime.of(2020, 1, 20, 0, 0, 0)),
                        new Tuple2<>("system_time_end", LocalDateTime.of(2020, 1, 20, 0, 0, 1)),
                        new Tuple2<>("stock", "GOOGL"),
                        new Tuple2<>("price", new BigDecimal("123.45")),
                        new Tuple2<>("volume", 100),
                        new Tuple2<>("buy_sell_flag", 'B'),
                        new Tuple2<>("market_limit_flag", 'M')
                )
        );

        final InsertQuery query = new InsertQuery<>(Trade.class);
        query.addFields(fields);

        final String sql = query.build();
        assertThat(sql).isEqualTo(
                "INSERT INTO reporting.trade_data (id, valid_time_start, valid_time_end, system_time_start, system_time_end, stock, price, volume, buy_sell_flag, market_limit_flag) VALUES ('769fb864-f3b7-4ca5-965e-bcff80088197', '2020-01-20', '2020-01-21', '2020-01-20 00:00:00.000000', '2020-01-20 00:00:01.000000', 'GOOGL', 123.45, 100, 'B', 'M')");
    }

    @Test
    void buildWorksForMultipleInputs() {
        final UUID id1 = UUID.fromString("769fb864-f3b7-4ca5-965e-bcff80088197");
        final UUID id2 = UUID.fromString("123fb864-f3b7-4ca5-965e-bcff80088123");
        final List<List<Tuple2<String, Object>>> fields = List.of(
                List.of(
                        new Tuple2<>("id", id1),
                        new Tuple2<>("valid_time_start", LocalDate.of(2020, 1, 20)),
                        new Tuple2<>("valid_time_end", LocalDate.of(2020, 1, 21)),
                        new Tuple2<>("system_time_start", LocalDateTime.of(2020, 1, 20, 0, 0, 0)),
                        new Tuple2<>("system_time_end", LocalDateTime.of(2020, 1, 20, 0, 0, 1)),
                        new Tuple2<>("stock", "GOOGL"),
                        new Tuple2<>("price", new BigDecimal("123.45")),
                        new Tuple2<>("volume", 100),
                        new Tuple2<>("buy_sell_flag", 'B'),
                        new Tuple2<>("market_limit_flag", 'M')
                ),
                List.of(
                        new Tuple2<>("id", id2),
                        new Tuple2<>("valid_time_start", LocalDate.of(2020, 2, 20)),
                        new Tuple2<>("valid_time_end", LocalDate.of(2020, 2, 21)),
                        new Tuple2<>("system_time_start", LocalDateTime.of(2020, 2, 20, 0, 0, 0)),
                        new Tuple2<>("system_time_end", LocalDateTime.of(2020, 2, 20, 0, 0, 1)),
                        new Tuple2<>("stock", "MSFT"),
                        new Tuple2<>("price", new BigDecimal("321.45")),
                        new Tuple2<>("volume", 200),
                        new Tuple2<>("buy_sell_flag", 'S'),
                        new Tuple2<>("market_limit_flag", 'L')
                )
        );

        final InsertQuery query = new InsertQuery<>(Trade.class);
        query.addFields(fields);

        final String sql = query.build();
        assertThat(sql).isEqualTo(
                "INSERT INTO reporting.trade_data (id, valid_time_start, valid_time_end, system_time_start, system_time_end, stock, price, volume, buy_sell_flag, market_limit_flag) " +
                        "VALUES ('769fb864-f3b7-4ca5-965e-bcff80088197', '2020-01-20', '2020-01-21', '2020-01-20 00:00:00.000000', '2020-01-20 00:00:01.000000', 'GOOGL', 123.45, 100, 'B', 'M'), " +
                        "('123fb864-f3b7-4ca5-965e-bcff80088123', '2020-02-20', '2020-02-21', '2020-02-20 00:00:00.000000', '2020-02-20 00:00:01.000000', 'MSFT', 321.45, 200, 'S', 'L')");
    }
}

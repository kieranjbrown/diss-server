package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UpdateQueryTest {

    @Test
    void constructorThrowsForInvalidInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new UpdateQuery<>(null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void buildReturnsCorrectQuery() {
        final List<Tuple2<String, Object>> fields = List.of(
                new Tuple2<>("valid_time_end", LocalDate.of(2020, 1, 21)),
                new Tuple2<>("system_time_end", LocalDateTime.of(2020, 1, 20, 0, 0, 1))
        );

        final List<SingleQueryFilter> filters = List.of(
                new SingleQueryFilter("id", QueryEquality.EQUALS, 5),
                new SingleQueryFilter("system_time_end", QueryEquality.EQUALS, LocalDateTime.of(9999, 12, 31, 0, 0, 0))
        );

        final UpdateQuery query = new UpdateQuery<>(Trade.class);
        query.addFields(fields);
        query.addFilters(filters);

        final String sql = query.build();
        assertThat(sql).isEqualTo(
                "UPDATE reporting.trade_data " +
                        "SET valid_time_end = '2020-01-21', system_time_end = '2020-01-20 00:00:01.000000' " +
                        "WHERE id = 5 AND system_time_end = '9999-12-31 00:00:00.000000'");
    }
}

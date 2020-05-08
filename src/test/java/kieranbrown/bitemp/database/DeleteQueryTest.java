package kieranbrown.bitemp.database;

import io.vavr.collection.List;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeleteQueryTest {

    @Test
    void constructorThrowsForInvalidInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new DeleteQuery<>(null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void buildReturnsCorrectQuery() {
        final List<QueryFilter> filters = List.of(
                new SingleQueryFilter("id", QueryEquality.EQUALS, 5),
                new SingleQueryFilter("valid_time_start", QueryEquality.LESS_THAN_EQUAL_TO, LocalDate.of(2020, 2, 3))
        );

        final DeleteQuery<Trade> query = new DeleteQuery<>(Trade.class);
        query.addFilters(filters);

        final String sql = query.build();
        assertThat(sql).isEqualTo(
                "DELETE FROM reporting.trade_data " +
                        "WHERE id = 5 AND valid_time_start <= '2020-02-03'");
    }
}

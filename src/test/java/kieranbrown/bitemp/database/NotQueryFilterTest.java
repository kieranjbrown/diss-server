package kieranbrown.bitemp.database;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NotQueryFilterTest {
    @Test
    void constructorThrowsForNullInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new NotQueryFilter(null)))
                .hasMessage("filter cannot be null");
    }

    @Test
    void constructsQueryCorrectly() {
        final NotQueryFilter notQueryFilter = new NotQueryFilter(
                new AndQueryFilter(
                        new SingleQueryFilter("valid_time_start", QueryEquality.GREATER_THAN_EQUAL_TO, LocalDate.of(2020, 1, 20)),
                        new SingleQueryFilter("valid_time_end", QueryEquality.GREATER_THAN_EQUAL_TO, LocalDate.of(2020, 1, 21))
                )
        );
        assertThat(notQueryFilter.getFilters()).isNotNull().isEqualTo("NOT (valid_time_start >= '2020-01-20' AND valid_time_end >= '2020-01-21')");
    }
}

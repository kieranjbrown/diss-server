package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SingleQueryFilterTest {
    @Test
    void constructorsThrowForNullInputs() {
        assertThat(assertThrows(NullPointerException.class, () -> new SingleQueryFilter(null)))
                .hasMessage("filter cannot be null");

        assertThat(assertThrows(NullPointerException.class, () -> new SingleQueryFilter(null, QueryEquality.EQUALS, 3)))
                .hasMessage("column cannot be null/blank");

        assertThat(assertThrows(NullPointerException.class, () -> new SingleQueryFilter(null, QueryEquality.EQUALS, 3)))
                .hasMessage("column cannot be null/blank");

        assertThat(assertThrows(NullPointerException.class, () -> new SingleQueryFilter(null, QueryEquality.EQUALS, 3)))
                .hasMessage("column cannot be null/blank");

        assertThat(assertThrows(NullPointerException.class, () -> new SingleQueryFilter("id", null, 3)))
                .hasMessage("condition cannot be null");

        assertThat(assertThrows(NullPointerException.class, () -> new SingleQueryFilter("id", QueryEquality.EQUALS, null)))
                .hasMessage("value cannot be null");
    }

    @Test
    void getFilterReturnsAllFilteredCorrectly() {
        assertThat(new SingleQueryFilter(new Tuple3<>("id", QueryEquality.EQUALS, 3)).getFilters()).isEqualTo("id = 3");
        assertThat(new SingleQueryFilter("id", QueryEquality.EQUALS, 3).getFilters()).isEqualTo("id = 3");
    }
}

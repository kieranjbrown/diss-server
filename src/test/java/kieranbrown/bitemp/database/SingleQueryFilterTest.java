package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import io.vavr.collection.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SingleQueryFilterTest {
    @Test
    void constructorsThrowForNullInputs() {
        assertThat(assertThrows(NullPointerException.class, () -> new SingleQueryFilter((Tuple3<String, QueryEquality, Object>[]) null)))
                .hasMessage("filters cannot be null");

        assertThat(assertThrows(NullPointerException.class, () -> new SingleQueryFilter((Tuple3<String, QueryEquality, Object>) null)))
                .hasMessage("filter cannot be null");

        assertThat(assertThrows(NullPointerException.class, () -> new SingleQueryFilter((List<Tuple3<String, QueryEquality, Object>>) null)))
                .hasMessage("filters cannot be null");
    }

    @Test
    void getFilterReturnsAllFilteredCorrectly() {
        assertThat(new SingleQueryFilter(new Tuple3<>("id", QueryEquality.EQUALS, 3)).getFilters()).isEqualTo("(id = 3)");
    }

    @Test
    void getFiltersReturnsAll() {
        final List<Tuple3<String, QueryEquality, Object>> filters = List.of(
                new Tuple3<>("id", QueryEquality.EQUALS, 3),
                new Tuple3<>("version", QueryEquality.GREATER_THAN_EQUAL_TO, 10)
        );

        assertThat(new SingleQueryFilter(filters).getFilters()).isEqualTo("(id = 3)")

//        assertThat(new SingleQueryFilter(filters).getFilters()).isNotNull().hasSize(2).containsAll(filters);
    }
}

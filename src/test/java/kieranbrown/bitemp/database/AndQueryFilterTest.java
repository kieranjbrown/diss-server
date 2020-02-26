package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import io.vavr.collection.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AndQueryFilterTest {
    @Test
    void constructorThrowsForNullArguments() {
        assertThat(assertThrows(NullPointerException.class, () -> new AndQueryFilter((Tuple3<String, QueryEquality, Object>[]) null)))
                .hasMessage("filters cannot be null");

        assertThat(assertThrows(NullPointerException.class, () -> new AndQueryFilter((List<QueryFilter>) null)))
                .hasMessage("filters cannot be null");
    }

    @Test
    void constructorsAssignFiltersAccordingly() {
        final List<QueryFilter> filters = List.of(
                new SingleQueryFilter(new Tuple3<>("id", QueryEquality.EQUALS, 3)),
                new SingleQueryFilter(new Tuple3<>("version", QueryEquality.GREATER_THAN_EQUAL_TO, 10))
        );

        assertThat(new AndQueryFilter(filters)).hasFieldOrPropertyWithValue("filterList", filters);

        assertThat(
                new AndQueryFilter(
                        new Tuple3<>("id", QueryEquality.EQUALS, 3),
                        new Tuple3<>("version", QueryEquality.GREATER_THAN_EQUAL_TO, 10))
        ).hasFieldOrPropertyWithValue("filterList", filters);
    }

    @Test
    void getFilersFormatsCorrectlyForSimpleFilters() {
        final List<QueryFilter> filters = List.of(
                new SingleQueryFilter(new Tuple3<>("id", QueryEquality.EQUALS, 3)),
                new SingleQueryFilter(new Tuple3<>("version", QueryEquality.GREATER_THAN_EQUAL_TO, 10))
        );

        final AndQueryFilter queryFilter = new AndQueryFilter(filters);
        assertThat(queryFilter.getFilters()).isNotNull().isEqualTo("(id = 3 AND version >= 10)");
    }

    @Test
    void getFilersFormatsCorrectlyForComplexFilters() {
        final List<QueryFilter> filters = List.of(
                new AndQueryFilter(
                        new Tuple3<>("id", QueryEquality.EQUALS, 3),
                        new Tuple3<>("valid_time_start", QueryEquality.GREATER_THAN_EQUAL_TO, 200)
                ),
                new SingleQueryFilter(new Tuple3<>("version", QueryEquality.GREATER_THAN_EQUAL_TO, 10))
        );

        final AndQueryFilter queryFilter = new AndQueryFilter(filters);
        assertThat(queryFilter.getFilters()).isNotNull().isEqualTo("((id = 3 AND valid_time_start >= 200) AND version >= 10)");
    }
}

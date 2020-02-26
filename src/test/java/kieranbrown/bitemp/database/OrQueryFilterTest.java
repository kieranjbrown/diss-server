package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import io.vavr.collection.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrQueryFilterTest {

    @Test
    void constructorThrowsForNullArguments() {
        assertThat(assertThrows(NullPointerException.class, () -> new OrQueryFilter((Tuple3<String, QueryEquality, Object>[]) null)))
                .hasMessage("filters cannot be null");

        assertThat(assertThrows(NullPointerException.class, () -> new OrQueryFilter((List<QueryFilter>) null)))
                .hasMessage("filters cannot be null");
    }

    @Test
    void constructorsAssignFiltersAccordingly() {
        final List<QueryFilter> filters = List.of(
                new SingleQueryFilter(new Tuple3<>("id", QueryEquality.EQUALS, 3)),
                new SingleQueryFilter(new Tuple3<>("version", QueryEquality.GREATER_THAN_EQUAL_TO, 10))
        );

        assertThat(new OrQueryFilter(filters)).hasFieldOrPropertyWithValue("filterList", filters);

        assertThat(
                new OrQueryFilter(
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

        final OrQueryFilter queryFilter = new OrQueryFilter(filters);
        assertThat(queryFilter.getFilters()).isNotNull().isEqualTo("(id = 3 OR version >= 10)");
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

        final OrQueryFilter queryFilter = new OrQueryFilter(filters);
        assertThat(queryFilter.getFilters()).isNotNull().isEqualTo("((id = 3 AND valid_time_start >= 200) OR version >= 10)");
    }
}
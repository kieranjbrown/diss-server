package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryBuilderFactoryTest {
    @Test
    void canCreateQueryForDistinctResult() {
        assertThat(QueryBuilderFactory.selectDistinct(Trade.class)).isNotNull()
                .hasFieldOrPropertyWithValue("query", new Query<>(QueryType.SELECT_DISTINCT, Trade.class))
                .hasFieldOrPropertyWithValue("queryClass", Trade.class);
    }

    @Test
    void selectDistinctThrowsForNullValues() {
        assertThat(assertThrows(NullPointerException.class, () -> QueryBuilderFactory.selectDistinct(null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void canCreateQueryForMultipleResults() {
        assertThat(QueryBuilderFactory.select(Trade.class)).isNotNull()
                .hasFieldOrPropertyWithValue("query", new Query<>(QueryType.SELECT, Trade.class))
                .hasFieldOrPropertyWithValue("queryClass", Trade.class);
    }

    @Test
    void selectThrowsForNullValues() {
        assertThat(assertThrows(NullPointerException.class, () -> QueryBuilderFactory.select(null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void canCreateQueryForInsert() {
        assertThat(QueryBuilderFactory.insert(Trade.class)).isNotNull()
                .hasFieldOrPropertyWithValue("query", new Query<>(QueryType.INSERT, Trade.class))
                .hasFieldOrPropertyWithValue("queryClass", Trade.class);
    }

    @Test
    void insertThrowsForNullValues() {
        assertThat(assertThrows(NullPointerException.class, () -> QueryBuilderFactory.insert(null)))
                .hasMessage("class cannot be null");
    }
}

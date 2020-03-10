package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryBuilderFactoryTest {
    @Test
    void canCreateQueryForMultipleResults() {
        assertThat(QueryBuilderFactory.select(Trade.class)).isNotNull()
                .hasFieldOrPropertyWithValue("query", new SelectQuery<>(QueryType.SELECT, Trade.class))
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
                .hasFieldOrPropertyWithValue("queryClass", Trade.class);
    }

    @Test
    void insertThrowsForNullValues() {
        assertThat(assertThrows(NullPointerException.class, () -> QueryBuilderFactory.insert(null)))
                .hasMessage("class cannot be null");
    }

    @Test
    void canCreateUpdateQueryBuilder() {
        assertThat(QueryBuilderFactory.update(Trade.class)).isNotNull();
    }

    @Test
    void updateThrowsForNullInput() {
        assertThat(assertThrows(NullPointerException.class, () -> QueryBuilderFactory.update(null)))
                .hasMessage("class cannot be null");
    }
}

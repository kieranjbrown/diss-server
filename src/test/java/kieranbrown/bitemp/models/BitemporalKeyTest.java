package kieranbrown.bitemp.models;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BitemporalKeyTest {

    @Test
    void constructorThrowsForInvalidInput() {
        assertThat(assertThrows(NullPointerException.class, () -> new BitemporalKey(null, 1)))
                .hasMessage("tradeId cannot be null");

        assertThat(assertThrows(IllegalArgumentException.class, () -> new BitemporalKey(UUID.randomUUID(), -1)))
                .hasMessage("version cannot be negative");
    }

    @Test
    void setTradeIdThrowsForNullId() {
        final BitemporalKey key = new BitemporalKey(UUID.randomUUID(), 2000);
        assertThat(assertThrows(NullPointerException.class, () -> key.setTradeId(null)))
                .hasMessage("tradeId cannot be null");
    }

    @Test
    void setVersionThrowsForInvalidVersion() {
        final BitemporalKey key = new BitemporalKey(UUID.randomUUID(), 2000);
        assertThat(assertThrows(IllegalArgumentException.class, () -> key.setVersion(-5000)))
                .hasMessage("version cannot be negative");
    }
}

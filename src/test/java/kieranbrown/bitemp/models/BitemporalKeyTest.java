package kieranbrown.bitemp.models;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BitemporalKeyTest {

    @Test
    void setTradeIdThrowsForNullId() {
        final BitemporalKey.Builder builder = new BitemporalKey.Builder()
                .setTradeId(UUID.randomUUID())
                .setVersion(2000);
        assertThat(assertThrows(NullPointerException.class, () -> builder.setTradeId(null)))
                .hasMessage("id cannot be null");
    }

    @Test
    void setVersionThrowsForInvalidVersion() {
        final BitemporalKey.Builder builder = new BitemporalKey.Builder()
                .setTradeId(UUID.randomUUID())
                .setVersion(2000);
        assertThat(assertThrows(IllegalArgumentException.class, () -> builder.setVersion(-5000)))
                .hasMessage("version cannot be negative");
    }

    @Test
    void builderSetsValues() {
        final UUID tradeId = UUID.randomUUID();
        final BitemporalKey key = new BitemporalKey.Builder()
                .setTradeId(tradeId)
                .setVersion(2000)
                .build();

        assertThat(key.getId()).isNotNull().isEqualTo(tradeId);
        assertThat(key.getVersion()).isEqualTo(2000);
    }
}

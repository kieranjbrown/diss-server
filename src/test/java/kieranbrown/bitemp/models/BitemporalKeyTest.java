package kieranbrown.bitemp.models;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BitemporalKeyTest {

    @Test
    void setTradeIdThrowsForNullId() {
        assertThat(assertThrows(NullPointerException.class, () -> new BitemporalKey.Builder().setTradeId(null)))
                .hasMessage("id cannot be null");
    }

    @Test
    void setVersionThrowsForNullValidTimeStart() {
        assertThat(assertThrows(NullPointerException.class, () -> new BitemporalKey.Builder().setValidTimeStart(null)))
                .hasMessage("validTimeStart cannot be null");
    }

    @Test
    void setVersionThrowsForNullValidTimeEnd() {
        assertThat(assertThrows(NullPointerException.class, () -> new BitemporalKey.Builder().setValidTimeEnd(null)))
                .hasMessage("validTimeEnd cannot be null");
    }

    @Test
    void builderSetsValues() {
        final UUID tradeId = UUID.randomUUID();
        final BitemporalKey key = new BitemporalKey.Builder()
                .setTradeId(tradeId)
                .setValidTimeStart(LocalDate.of(2020, 10, 20))
                .setValidTimeEnd(LocalDate.of(2020, 10, 21))
                .build();

        assertThat(key.getId()).isNotNull().isEqualTo(tradeId);
        assertThat(key.getValidTimeStart()).isEqualTo(LocalDate.of(2020, 10, 20));
        assertThat(key.getValidTimeEnd()).isEqualTo(LocalDate.of(2020, 10, 21));
    }
}

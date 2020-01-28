package kieranbrown.bitemp.models;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.Validate.isTrue;

@Embeddable
public class BitemporalKey implements Serializable {
    @Column(name = "trade_id", nullable = false)
    private UUID tradeId;

    @Column(name = "version", nullable = false)
    private int version;

    public BitemporalKey(final UUID tradeId,
                         final int version) {
        this.tradeId = requireNonNull(tradeId, "tradeId cannot be null");
        isTrue(version >= 0, "version cannot be negative");
        this.version = version;
    }

    public UUID getTradeId() {
        return tradeId;
    }

    public void setTradeId(final UUID tradeId) {
        this.tradeId = requireNonNull(tradeId, "tradeId cannot be null");
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        isTrue(version >= 0, "version cannot be negative");
        this.version = version;
    }
}

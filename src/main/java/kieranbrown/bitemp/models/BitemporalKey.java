package kieranbrown.bitemp.models;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.Validate.isTrue;

@Embeddable
public class BitemporalKey implements Serializable {
    @Column(name = "trade_id", nullable = false)
    private final UUID tradeId;

    @Column(name = "version", nullable = false)
    private final int version;

    private BitemporalKey(final UUID tradeId,
                          final int version) {
        this.tradeId = tradeId;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        BitemporalKey that = (BitemporalKey) o;

        return new EqualsBuilder()
                .append(version, that.version)
                .append(tradeId, that.tradeId)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(tradeId)
                .append(version)
                .toHashCode();
    }

    public static class Builder {
        private UUID tradeId;
        private int version;

        public Builder setTradeId(final UUID tradeId) {
            this.tradeId = requireNonNull(tradeId, "tradeId cannot be null");
            return this;
        }

        public Builder setVersion(final int version) {
            isTrue(version >= 0, "version cannot be negative");
            this.version = version;
            return this;
        }

        public BitemporalKey build() {
            return new BitemporalKey(tradeId, version);
        }
    }

    public UUID getTradeId() {
        return tradeId;
    }

    public int getVersion() {
        return version;
    }
}

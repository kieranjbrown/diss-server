package kieranbrown.bitemp.models;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.Validate.isTrue;

@Embeddable
public class BitemporalKey implements Serializable {
    @Column(name = "id", nullable = false)
    private UUID id;

    //TODO: This should probably be valid time start / end? gonna make updates very difficult presumably
    @Column(name = "version", nullable = false)
    private int version;

    private BitemporalKey() {
    }

    private BitemporalKey(final UUID id,
                          final int version) {
        this.id = id;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        BitemporalKey that = (BitemporalKey) o;

        return new EqualsBuilder()
                .append(version, that.version)
                .append(id, that.id)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(version)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("version", version)
                .toString();
    }

    public UUID getId() {
        return id;
    }

    public static class Builder {
        private UUID tradeId;
        private int version;

        public Builder setTradeId(final UUID tradeId) {
            this.tradeId = requireNonNull(tradeId, "id cannot be null");
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

    public int getVersion() {
        return version;
    }
}

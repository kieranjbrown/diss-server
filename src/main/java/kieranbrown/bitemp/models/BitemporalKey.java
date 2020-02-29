package kieranbrown.bitemp.models;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

@Embeddable
public class BitemporalKey implements Serializable {
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "valid_time_start", nullable = false)
    private LocalDate validTimeStart;

    @Column(name = "valid_time_end", nullable = false)
    private LocalDate validTimeEnd;

    private BitemporalKey() {
    }

    private BitemporalKey(final UUID id,
                          final LocalDate validTimeStart,
                          final LocalDate validTimeEnd) {
        this.id = id;
        this.validTimeStart = validTimeStart;
        this.validTimeEnd = validTimeEnd;
    }

    public UUID getId() {
        return id;
    }

    public LocalDate getValidTimeStart() {
        return validTimeStart;
    }

    public LocalDate getValidTimeEnd() {
        return validTimeEnd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        BitemporalKey that = (BitemporalKey) o;

        return new EqualsBuilder()
                .append(validTimeStart, that.validTimeStart)
                .append(validTimeEnd, that.validTimeEnd)
                .append(id, that.id)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(validTimeStart)
                .append(validTimeEnd)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("valid_time_start", validTimeStart)
                .append("valid_time_end", validTimeEnd)
                .toString();
    }

    public static class Builder {
        private UUID tradeId;
        private LocalDate validTimeStart;
        private LocalDate validTimeEnd;

        public Builder setTradeId(final UUID tradeId) {
            this.tradeId = requireNonNull(tradeId, "id cannot be null");
            return this;
        }

        public Builder setValidTimeStart(final LocalDate validTimeStart) {
            this.validTimeStart = requireNonNull(validTimeStart, "validTimeStart cannot be null");
            return this;
        }

        public Builder setValidTimeEnd(final LocalDate validTimeEnd) {
            this.validTimeEnd = requireNonNull(validTimeEnd, "validTimeEnd cannot be null");
            return this;
        }

        public BitemporalKey build() {
            return new BitemporalKey(tradeId, validTimeStart, validTimeEnd);
        }
    }
}

package kieranbrown.bitemp.models;

import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import java.time.LocalDate;
import java.time.LocalDateTime;

@MappedSuperclass
@SuppressWarnings("unchecked")
public abstract class BitemporalModel<T extends BitemporalModel> {
    @EmbeddedId
    protected BitemporalKey tradeKey;

    //TODO: Should these be wrapped into another class?
    @Column(nullable = false, name = "valid_time_start")
    protected LocalDate validTimeStart;

    @Column(name = "valid_time_end")
    protected LocalDate validTimeEnd;

    @Column(name = "system_time_start")
    protected LocalDateTime systemTimeStart;

    @Column(name = "system_time_end")
    protected LocalDateTime systemTimeEnd;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tradeKey", tradeKey)
                .append("validTimeStart", validTimeStart)
                .append("validTimeEnd", validTimeEnd)
                .append("systemTimeStart", systemTimeStart)
                .append("systemTimeEnd", systemTimeEnd)
                .toString();
    }

    public BitemporalKey getTradeKey() {
        return tradeKey;
    }

    public T setTradeKey(final BitemporalKey tradeKey) {
        this.tradeKey = tradeKey;
        return (T) this;
    }

    public LocalDate getValidTimeStart() {
        return LocalDate.from(validTimeStart);
    }

    public T setValidTimeStart(final LocalDate validTimeStart) {
        this.validTimeStart = LocalDate.from(validTimeStart);
        return (T) this;
    }

    public LocalDate getValidTimeEnd() {
        return LocalDate.from(validTimeEnd);
    }

    public T setValidTimeEnd(final LocalDate validTimeEnd) {
        this.validTimeEnd = LocalDate.from(validTimeEnd);
        return (T) this;
    }

    public LocalDateTime getSystemTimeStart() {
        return LocalDateTime.from(systemTimeStart);
    }

    public T setSystemTimeStart(final LocalDateTime systemTimeStart) {
        this.systemTimeStart = LocalDateTime.from(systemTimeStart);
        return (T) this;
    }

    public LocalDateTime getSystemTimeEnd() {
        return LocalDateTime.from(systemTimeEnd);
    }

    public T setSystemTimeEnd(final LocalDateTime systemTimeEnd) {
        this.systemTimeEnd = LocalDateTime.from(systemTimeEnd);
        return (T) this;
    }

    @PrePersist
    void prePersist() {
        //TODO: there has to be a better way of doing this
        //TODO: is this needed anymore now not using repositories?
        if (validTimeEnd == null) {
            validTimeEnd = LocalDate.of(9999, 12, 31);
        }
        if (systemTimeStart == null) {
            systemTimeStart = LocalDateTime.now();
        }
        if (systemTimeEnd == null) {
            systemTimeEnd = LocalDateTime.of(9999, 12, 31, 0, 0, 0);
        }
    }
}

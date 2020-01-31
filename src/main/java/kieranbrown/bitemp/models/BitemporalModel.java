package kieranbrown.bitemp.models;

import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import java.util.Date;

@MappedSuperclass
@SuppressWarnings("unchecked")
public abstract class BitemporalModel<T extends BitemporalModel> {
    @EmbeddedId
    protected BitemporalKey tradeKey;

    //TODO: Should these be wrapped into another class?
    @Column(nullable = false, name = "valid_time_start")
    protected Date validTimeStart;

    @Column(name = "valid_time_end")
    protected Date validTimeEnd;

    @Column(name = "system_time_start")
    protected Date systemTimeStart;

    @Column(name = "system_time_end")
    protected Date systemTimeEnd;

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

    public Date getValidTimeStart() {
        return new Date(validTimeStart.getTime());
    }

    public T setValidTimeStart(final Date validTimeStart) {
        this.validTimeStart = new Date(validTimeStart.getTime());
        return (T) this;
    }

    public Date getValidTimeEnd() {
        return new Date(validTimeEnd.getTime());
    }

    public T setValidTimeEnd(final Date validTimeEnd) {
        this.validTimeEnd = new Date(validTimeEnd.getTime());
        return (T) this;
    }

    public Date getSystemTimeStart() {
        return new Date(systemTimeStart.getTime());
    }

    public T setSystemTimeStart(final Date systemTimeStart) {
        this.systemTimeStart = new Date(systemTimeStart.getTime());
        return (T) this;
    }

    public Date getSystemTimeEnd() {
        return new Date(systemTimeEnd.getTime());
    }

    public T setSystemTimeEnd(final Date systemTimeEnd) {
        this.systemTimeEnd = new Date(systemTimeEnd.getTime());
        return (T) this;
    }

    @PrePersist
    void prePersist() {
        //TODO: there has to be a better way of doing this
        //TODO: convert these all to LocalDateTime
        if (validTimeEnd == null) {
            validTimeEnd = new Date(9999, 12, 31, 0, 0, 0);
        }
        if (systemTimeStart == null) {
            systemTimeStart = new Date();
        }
        if (systemTimeEnd == null) {
            systemTimeEnd = new Date(9999, 12, 31, 0, 0, 0);
        }
    }
}

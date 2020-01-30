package kieranbrown.bitemp.models;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.MappedSuperclass;
import java.util.Date;

@MappedSuperclass
@SuppressWarnings("unchecked")
public abstract class BitemporalModel<T extends BitemporalModel> {
    @EmbeddedId
    private BitemporalKey tradeKey;

    //TODO: Should these be wrapped into another class?
    @Column(nullable = false)
    private Date validTimeStart;

    @Column
    private Date validTimeEnd;

    @Column(nullable = false)
    private Date systemTimeStart;

    @Column
    private Date systemTimeEnd;

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
}

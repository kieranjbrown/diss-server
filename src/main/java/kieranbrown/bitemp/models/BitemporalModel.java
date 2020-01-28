package kieranbrown.bitemp.models;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.MappedSuperclass;
import java.util.Date;

@MappedSuperclass
public abstract class BitemporalModel {
    @EmbeddedId
    private BitemporalKey tradeKey;

    //TODO: Should these be wrapped into another class?
    @Column(nullable = false)
    private Date validTimeStart;

    @Column(nullable = false)
    private Date validTimeEnd;

    @Column(nullable = false)
    private Date systemTimeStart;

    @Column(nullable = false)
    private Date systemTimeEnd;

    public BitemporalKey getTradeKey() {
        return tradeKey;
    }

    public void setTradeKey(final BitemporalKey tradeKey) {
        this.tradeKey = tradeKey;
    }

    public Date getValidTimeStart() {
        return new Date(validTimeStart.getTime());
    }

    public void setValidTimeStart(final Date validTimeStart) {
        this.validTimeStart = new Date(validTimeStart.getTime());
    }

    public Date getValidTimeEnd() {
        return new Date(validTimeEnd.getTime());
    }

    public void setValidTimeEnd(final Date validTimeEnd) {
        this.validTimeEnd = new Date(validTimeEnd.getTime());
    }

    public Date getSystemTimeStart() {
        return new Date(systemTimeStart.getTime());
    }

    public void setSystemTimeStart(final Date systemTimeStart) {
        this.systemTimeStart = new Date(systemTimeStart.getTime());
    }

    public Date getSystemTimeEnd() {
        return new Date(systemTimeEnd.getTime());
    }

    public void setSystemTimeEnd(final Date systemTimeEnd) {
        this.systemTimeEnd = new Date(systemTimeEnd.getTime());
    }
}

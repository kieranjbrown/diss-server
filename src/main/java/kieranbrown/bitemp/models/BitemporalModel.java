package kieranbrown.bitemp.models;

import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.CsvDate;
import com.opencsv.bean.CsvRecurse;
import kieranbrown.bitemp.utils.Constants;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.MappedSuperclass;
import java.time.LocalDateTime;

@MappedSuperclass
@SuppressWarnings("unchecked")
public abstract class BitemporalModel<T extends BitemporalModel> {
    @EmbeddedId
    @CsvRecurse
    private BitemporalKey bitemporalKey;

    @Column(name = "system_time_start")
    @CsvBindByPosition(position = 6)
    @CsvDate
    private LocalDateTime systemTimeStart;

    @Column(name = "system_time_end")
    @CsvBindByPosition(position = 5)
    @CsvDate
    private LocalDateTime systemTimeEnd = Constants.MARIADB_END_SYSTEM_TIME;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tradeKey", bitemporalKey)
                .append("systemTimeStart", systemTimeStart)
                .append("systemTimeEnd", systemTimeEnd)
                .toString();
    }

    public BitemporalKey getBitemporalKey() {
        return bitemporalKey;
    }

    public T setBitemporalKey(final BitemporalKey bitemporalKey) {
        this.bitemporalKey = bitemporalKey;
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
}

package kieranbrown.bitemp.models;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;
import java.util.UUID;

@Embeddable
class TradeKey implements Serializable {
    @Column(name = "trade_id", nullable = false)
    private UUID tradeId;

    @Column(name = "version", nullable = false)
    private int version;
}

package kieranbrown.bitemp.models;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.util.Date;

@Entity
@Table(name = "trade_data", schema = "reporting")
public class Trade {
    @EmbeddedId
    private TradeKey tradeKey;

    @Column(nullable = false)
    private String stock;

    @Column(nullable = false)
    private BigDecimal price;

    @Column(nullable = false)
    private int volume;

    //TODO: Should these be wrapped into another class?
    @Column(nullable = false)
    private Date validTimeStart;

    @Column(nullable = false)
    private Date validTimeEnd;

    @Column(nullable = false)
    private Date systemTimeStart;

    @Column(nullable = false)
    private Date systemTimeEnd;

    //TODO: enum?
    @Column(nullable = false)
    private char buySellFlag;

    //TODO: enum?
    @Column(nullable = false)
    private char marketLimitFlag;
}

package kieranbrown.bitemp.models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.math.BigDecimal;

@Entity
@Table(name = "trade_data", schema = "reporting")
public class Trade extends BitemporalModel<Trade> {

//    public static class Builder {
//        private BitemporalKey key;
//        private String stock;
//        private BigDecimal price;
//
//    }

    @Column(nullable = false)
    private String stock;

    @Column(nullable = false)
    private BigDecimal price;

    @Column(nullable = false)
    private int volume;

    //TODO: enum?
    @Column(nullable = false)
    private char buySellFlag;

    //TODO: enum?
    @Column(nullable = false)
    private char marketLimitFlag;

    public String getStock() {
        return stock;
    }

    public Trade setStock(final String stock) {
        this.stock = stock;
        return this;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public Trade setPrice(final BigDecimal price) {
        this.price = price;
        return this;
    }

    public int getVolume() {
        return volume;
    }

    public Trade setVolume(final int volume) {
        this.volume = volume;
        return this;
    }

    public char getBuySellFlag() {
        return buySellFlag;
    }

    public Trade setBuySellFlag(final char buySellFlag) {
        this.buySellFlag = buySellFlag;
        return this;
    }

    public char getMarketLimitFlag() {
        return marketLimitFlag;
    }

    public Trade setMarketLimitFlag(final char marketLimitFlag) {
        this.marketLimitFlag = marketLimitFlag;
        return this;
    }
}

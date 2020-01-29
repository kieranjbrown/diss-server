package kieranbrown.bitemp.models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.math.BigDecimal;

@Entity
@Table(name = "trade_data", schema = "reporting")
public class Trade extends BitemporalModel {

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

    public void setStock(final String stock) {
        this.stock = stock;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(final BigDecimal price) {
        this.price = price;
    }

    public int getVolume() {
        return volume;
    }

    public void setVolume(final int volume) {
        this.volume = volume;
    }

    public char getBuySellFlag() {
        return buySellFlag;
    }

    public void setBuySellFlag(final char buySellFlag) {
        this.buySellFlag = buySellFlag;
    }

    public char getMarketLimitFlag() {
        return marketLimitFlag;
    }

    public void setMarketLimitFlag(final char marketLimitFlag) {
        this.marketLimitFlag = marketLimitFlag;
    }
}

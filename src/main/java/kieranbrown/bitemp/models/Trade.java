package kieranbrown.bitemp.models;

import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import java.math.BigDecimal;

@Entity(name = "reporting.trade_data")
public class Trade extends BitemporalModel<Trade> {

    @Column(nullable = false)
    private String stock;

    @Column(nullable = false)
    private BigDecimal price;

    @Column(nullable = false)
    private int volume;

    //TODO: enum?
    @Column(nullable = false, name = "buy_sell_flag")
    private char buySellFlag;

    //TODO: enum?
    @Column(nullable = false, name = "market_limit_flag")
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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("stock", stock)
                .append("price", price)
                .append("volume", volume)
                .append("buySellFlag", buySellFlag)
                .append("marketLimitFlag", marketLimitFlag)
                .append("identifyingInformation", super.toString())
                .toString();
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

package entity;

import java.io.Serializable;

public class StockQuotationInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    //股票代码
    private String stockCode;
    //股票名称
    private String stockName;
    //交易时间
    private long tradeTime;
    //昨日收盘价格
    private float preClosePrice;
    //开盘价
    private float openPrice;
    //当前价格
    private float currentPrice;
    //今日最高价格
    private float highPrice;
    //今日最低价格
    private float lowPrice;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getStockCode() {
        return stockCode;
    }

    public void setStockCode(String stockCode) {
        this.stockCode = stockCode;
    }

    public String getStockName() {
        return stockName;
    }

    public void setStockName(String stockName) {
        this.stockName = stockName;
    }

    public long getTradeTime() {
        return tradeTime;
    }

    public void setTradeTime(long tradeTime) {
        this.tradeTime = tradeTime;
    }

    public float getPreClosePrice() {
        return preClosePrice;
    }

    public void setPreClosePrice(float preClosePrice) {
        this.preClosePrice = preClosePrice;
    }

    public float getOpenPrice() {
        return openPrice;
    }

    public void setOpenPrice(float openPrice) {
        this.openPrice = openPrice;
    }

    public float getCurrentPrice() {
        return currentPrice;
    }

    public void setCurrentPrice(float currentPrice) {
        this.currentPrice = currentPrice;
    }

    public float getHighPrice() {
        return highPrice;
    }

    public void setHighPrice(float highPrice) {
        this.highPrice = highPrice;
    }

    public float getLowPrice() {
        return lowPrice;
    }

    public void setLowPrice(float lowPrice) {
        this.lowPrice = lowPrice;
    }

    @Override
    public String toString() {
        return "StockQuotationInfo{" +
                "stockCode='" + stockCode + '\'' +
                ", stockName='" + stockName + '\'' +
                ", tradeTime=" + tradeTime +
                ", preClosePrice=" + preClosePrice +
                ", openPrice=" + openPrice +
                ", currentPrice=" + currentPrice +
                ", highPrice=" + highPrice +
                ", lowPrice=" + lowPrice +
                '}';
    }
}

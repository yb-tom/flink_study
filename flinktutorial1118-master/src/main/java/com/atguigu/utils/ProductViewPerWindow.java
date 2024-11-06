package com.atguigu.utils;

import java.sql.Timestamp;

public class ProductViewPerWindow {
    public String productId;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public ProductViewPerWindow() {
    }

    public ProductViewPerWindow(String productId, Long count, Long windowStartTime, Long windowEndTime) {
        this.productId = productId;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "ProductViewPerWindow{" +
                "productId='" + productId + '\'' +
                ", count=" + count +
                ", windowStartTime=" + new Timestamp(windowStartTime) +
                ", windowEndTime=" + new Timestamp(windowEndTime) +
                '}';
    }
}

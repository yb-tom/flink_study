package com.atguigu.utils;

import java.sql.Timestamp;

// 必须是公有类
public class Event {
    // 所有字段必须是public
    public String key;
    public String value;
    public Long ts;

    // 空构造器
    public Event() {
    }

    public Event(String key, String value, Long ts) {
        this.key = key;
        this.value = value;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}

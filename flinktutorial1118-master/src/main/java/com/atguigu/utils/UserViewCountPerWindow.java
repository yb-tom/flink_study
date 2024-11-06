package com.atguigu.utils;

import java.sql.Timestamp;

public class UserViewCountPerWindow {
    public String username;
    public Integer count;
    public Long windowStartTime;
    public Long windowEndTime;

    public UserViewCountPerWindow() {
    }

    public UserViewCountPerWindow(String username, Integer count, Long windowStartTime, Long windowEndTime) {
        this.username = username;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "UserViewCountPerWindow{" +
                "username='" + username + '\'' +
                ", count=" + count +
                ", windowStartTime=" + new Timestamp(windowStartTime) +
                ", windowEndTime=" + new Timestamp(windowEndTime) +
                '}';
    }
}

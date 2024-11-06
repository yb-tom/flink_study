package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Example12 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        streamTableEnvironment
                .executeSql(
                        "create table clicks (`user` STRING, `url` STRING, " +
                                "ts TIMESTAMP(3)," +
                                "WATERMARK FOR ts AS ts - INTERVAL '3' SECONDS) WITH (" +
                                "'connector' = 'filesystem'," +
                                "'path' = '/home/zuoyuan/flinktutorial1118/src/main/resources/clicks.csv'," +
                                "'format' = 'csv'" +
                                ")"
                );

        streamTableEnvironment
                .executeSql(
                        "create table ResultTable (`user` STRING, cnt BIGINT, endT TIMESTAMP(3)) WITH (" +
                                "'connector' = 'print'" +
                                ")"
                );

        streamTableEnvironment
                .executeSql(
                        "insert into ResultTable " +
                                "select user, count(user) as cnt, " +
                                "TUMBLE_END(ts, INTERVAL '1' HOURS) as endT " +
                                "from clicks group by user, TUMBLE(ts, INTERVAL '1' HOURS)"
                );
    }
}

package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Example11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // 创建输入表，数据源是文件
        streamTableEnvironment
                .executeSql(
                        "create table clicks (`user` STRING, `url` STRING) " +
                                "WITH (" +
                                "'connector' = 'filesystem'," +
                                "'path' = '/home/zuoyuan/flinktutorial1118/src/main/resources/file.csv'," +
                                "'format' = 'csv'" +
                                ")"
                );

        // 创建输出表
        streamTableEnvironment
                .executeSql(
                        "create table ResultTable (`user` STRING, `cnt` BIGINT) " +
                                "WITH ('connector' = 'print')"
                );

        // 将查询结果写入输出表
        streamTableEnvironment
                .executeSql(
                        "insert into ResultTable " +
                                "select user, count(user) as cnt from clicks group by user"
                );

        // 注意不要写env.execute()!!!
    }
}

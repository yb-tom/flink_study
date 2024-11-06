package com.atguigu.day07;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        env
                .readTextFile("/home/zuoyuan/flinktutorial1118/src/main/resources/UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "userbehavior-1118",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}

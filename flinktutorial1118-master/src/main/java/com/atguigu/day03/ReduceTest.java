package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        stream.keyBy(r -> 1).sum(0).print("sum");
        stream.keyBy(r -> 1).max(0).print("max");
        stream.keyBy(r -> 1).min(0).print("min");

        env.execute();
    }
}

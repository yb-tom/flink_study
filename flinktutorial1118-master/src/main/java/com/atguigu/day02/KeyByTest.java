package com.atguigu.day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .keyBy(r -> r % 3)
                // sum(0)的底层实现
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                })
                .setParallelism(4)
                .print()
                .setParallelism(4);

        env.execute();
    }
}

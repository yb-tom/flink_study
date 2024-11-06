package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PhysicalPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .broadcast()
                .print("广播").setParallelism(4);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .shuffle()
                .print("随机发送").setParallelism(2);

        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .rebalance() // round-robin
                .print("轮询发送到下游的所有并行子任务").setParallelism(4);

        env
                .fromElements(1,2,3,4)
                .global()
                .print("将所有数据发送到第1个(索引是0)并行子任务").setParallelism(4);

        env.execute();
    }
}

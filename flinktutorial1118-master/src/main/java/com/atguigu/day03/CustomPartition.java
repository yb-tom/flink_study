package com.atguigu.day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 自定义分区算子
public class CustomPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 将key为0的发送到第0个并行子任务
        // 将key为1的发送到第1个并行子任务
        // 将key为2的发送到第2个并行子任务
        env
                .fromElements(1,2,3,4,5,6,7,8).setParallelism(1)
                .partitionCustom(
                        new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                // partition的返回值是将要发送到的print的并行子任务的索引
                                return key; // 将数据发送到索引==key的print的并行子任务中
                            }
                        },
                        // 为输入数据指定key
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer in) throws Exception {
                                return in % 3; // 将输入数据除以3的余数指定为key
                            }
                        }
                )
                .print().setParallelism(4);

        env.execute();
    }
}

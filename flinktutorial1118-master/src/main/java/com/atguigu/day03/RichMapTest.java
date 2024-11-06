package com.atguigu.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 当前的并行子任务的索引
                        int idx = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println("并行子任务索引：" + idx + " 生命周期开始");
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * value;
                    }

                    @Override
                    public void close() throws Exception {
                        // 当前的并行子任务的索引
                        int idx = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println("并行子任务索引：" + idx + " 生命周期结束");
                    }
                }).setParallelism(1)
                .print().setParallelism(1);

        env.execute();
    }
}

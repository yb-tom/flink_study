package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class PhysicalPartition1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        // source算子的第0个并行子任务发送：2,4,6,8
                        // source算子的第1个并行子任务发送：1,3,5,7
                        int idx = getRuntimeContext().getIndexOfThisSubtask();
                        for (int i = 1; i < 9; i++) {
                            if (i % 2 == idx) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rescale()
                .print("rescale")
                .setParallelism(4);

        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        // source算子的第0个并行子任务发送：2,4,6,8
                        // source算子的第1个并行子任务发送：1,3,5,7
                        int idx = getRuntimeContext().getIndexOfThisSubtask();
                        for (int i = 1; i < 9; i++) {
                            if (i % 2 == idx) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rebalance()
                .print("rebalance")
                .setParallelism(4);

        env.execute();
    }
}

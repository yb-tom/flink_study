package com.atguigu.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class ValueStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(10000));
                            Thread.sleep(100L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(r -> true)
                // process算子的并行子任务的数量是1
                .process(new KeyedProcessFunction<Boolean, Integer, Statistic>() {
                    // 声明状态变量
                    private ValueState<Statistic> accumulator;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 实例化状态变量
                        accumulator = getRuntimeContext().getState(
                                new ValueStateDescriptor<Statistic>(
                                        "acc", // 状态变量在状态后端的名字
                                        Types.POJO(Statistic.class) // 泛型
                                )
                        );
                    }

                    @Override
                    public void processElement(Integer in, Context ctx, Collector<Statistic> out) throws Exception {
                        // 如果输入数据in的key所对应的状态变量为空
                        if (accumulator.value() == null) {
                            // 更新输入数据in的key所对应的状态变量
                            accumulator.update(new Statistic(
                                    in,
                                    in,
                                    in,
                                    1,
                                    in
                            ));
                        }
                        // 如果输入数据in的key所对应的状态变量不为空
                        else {
                            // 取出状态变量中的累加器
                            Statistic oldAcc = accumulator.value();
                            // 输入数据in和累加器进行聚合
                            Statistic newAcc = new Statistic(
                                    Math.min(in, oldAcc.min),
                                    Math.max(in, oldAcc.max),
                                    in + oldAcc.sum,
                                    1 + oldAcc.count,
                                    (in + oldAcc.sum) / (1 + oldAcc.count)
                            );
                            // 将新的累加器写回状态变量
                            accumulator.update(newAcc);
                        }

                        // 每来一条数据，输出一次统计结果
                        out.collect(accumulator.value());
                    }
                })
                .print();

        env.execute();
    }

    public static class Statistic {
        public Integer min;
        public Integer max;
        public Integer sum;
        public Integer count;
        public Integer avg;

        public Statistic() {
        }

        public Statistic(Integer min, Integer max, Integer sum, Integer count, Integer avg) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
            this.avg = avg;
        }

        @Override
        public String toString() {
            return "Statistic{" +
                    "min=" + min +
                    ", max=" + max +
                    ", sum=" + sum +
                    ", count=" + count +
                    ", avg=" + avg +
                    '}';
        }
    }
}

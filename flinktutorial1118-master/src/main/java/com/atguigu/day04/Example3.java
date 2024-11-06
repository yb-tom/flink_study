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

public class Example3 {
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
                .process(new KeyedProcessFunction<Boolean, Integer, Statistic>() {
                    private ValueState<Statistic> acc;
                    // flag标志位表示是否存在发送统计数据的定时器
                    private ValueState<Boolean> flag;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        acc = getRuntimeContext().getState(
                                new ValueStateDescriptor<Statistic>(
                                        "acc",
                                        Types.POJO(Statistic.class)
                                )
                        );
                        flag = getRuntimeContext().getState(
                                new ValueStateDescriptor<Boolean>(
                                        "flag",
                                        Types.BOOLEAN
                                )
                        );
                    }

                    // 负责聚合操作和注册定时器
                    @Override
                    public void processElement(Integer in, Context ctx, Collector<Statistic> out) throws Exception {
                        // 聚合逻辑
                        if (acc.value() == null) {
                            acc.update(new Statistic(in, in, in, 1, in));
                        } else {
                            Statistic oldAcc = acc.value();
                            acc.update(new Statistic(
                                    Math.min(in, oldAcc.min),
                                    Math.max(in, oldAcc.max),
                                    in + oldAcc.sum,
                                    1 + oldAcc.count,
                                    (in + oldAcc.sum) / (1 + oldAcc.count)
                            ));
                        }

                        if (flag.value() == null) {
                            // 如果flag.value()是空，说明不存在定时器
                            // 1. 注册定时器
                            long tenSecLater = ctx.timerService().currentProcessingTime() + 10 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(tenSecLater);
                            // 2. 将标志位flag置为true，表示定时器已经注册了
                            flag.update(true);
                        }
                    }

                    // 定时器负责发送统计数据
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Statistic> out) throws Exception {
                        out.collect(acc.value());
                        // 由于定时器onTimer已经触发，所以定时器不存在了，需要将标志位flag置为空
                        flag.clear();
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

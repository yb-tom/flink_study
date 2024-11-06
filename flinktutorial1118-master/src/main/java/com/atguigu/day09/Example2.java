package com.atguigu.day09;

import com.atguigu.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial1118/src/main/resources/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] arr = in.split(",");
                        UserBehavior userBehavior = new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                        if (userBehavior.type.equals("pv"))
                            out.collect(userBehavior);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Tuple2<BloomFilter<String>, Long>, Long> {
        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            return Tuple2.of(
                    BloomFilter.create(
                            Funnels.stringFunnel(Charsets.UTF_8), // 待去重的数据类型
                            100000, // 待去重的元素数量的规模
                            0.01 // 误判率1%
                    ),
                    // 由于布隆过滤器只能判断一个用户可能来过或者一定没来过
                    // 没有计数功能（也就是计算多少用户一定没来过）
                    // 所以加一个字段用来计数
                    0L
            );
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(UserBehavior in, Tuple2<BloomFilter<String>, Long> accumulator) {
            // 如果一个用户一定每来过，则计数值加一
            if (!accumulator.f0.mightContain(in.userId)) {
                accumulator.f0.put(in.userId); // 将计算出的下标对应的元素置为1
                accumulator.f1 += 1;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<String>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> a, Tuple2<BloomFilter<String>, Long> b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, Integer, TimeWindow> {
        @Override
        public void process(Integer integer, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect("窗口" + new Timestamp(context.window().getStart()) + "~" +
                    "" + new Timestamp(context.window().getEnd()) + "中的uv是：" +
                    "" + elements.iterator().next());
        }
    }
}

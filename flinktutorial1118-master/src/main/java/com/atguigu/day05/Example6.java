package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 每隔1分钟插入一次水位线
        env.getConfig().setAutoWatermarkInterval(60 * 1000L);

        // "a 1" => 事件时间是1s
        // "a 2" => 事件时间是2s
        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] arr = in.split(" ");
                        return Tuple2.of(
                                arr[0],
                                // 将数据中的秒级时间戳转换成毫秒级时间戳
                                Long.parseLong(arr[1]) * 1000L
                        );
                    }
                })
                // 在map算子输出的数据流中插入水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 最大延迟时间设置为5秒钟
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5)
                                )
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                                return element.f1; // 告诉flink哪一个字段是事件时间
                                            }
                                        })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        int count = 0;
                        for (Tuple2<String, Long> e : elements) count++;
                        out.collect("key: " + key + ", 在窗口：" +
                                "" + context.window().getStart() + "~" +
                                "" + context.window().getEnd() + " 里面有 " +
                                "" + count + " 条数据");
                    }
                })
                .print();

        env.execute();
    }
}

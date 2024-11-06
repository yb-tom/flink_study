package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> in, Context ctx, Collector<String> out) throws Exception {
                        out.collect("数据" + in + "到达，事件时间是：" +
                                "" + new Timestamp(in.f1) + ", 当前process算子的水位线是：" +
                                "" + ctx.timerService().currentWatermark());
                        ctx.timerService().registerEventTimeTimer(
                                in.f1 + 9999L
                        );
                        out.collect("注册了时间戳是：" + new Timestamp(in.f1 + 9999L) + "的定时器");
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(new Timestamp(timestamp) + "的定时器触发了！" +
                                "当前process算子的水位线是：" + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}

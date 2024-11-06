package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class Example8 {
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
                        new WatermarkStrategy<Tuple2<String, Long>>() {
                            @Override
                            public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1; // 告诉flink事件时间是哪一个字段
                                    }
                                };
                            }

                            @Override
                            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Tuple2<String, Long>>() {
                                    // 最大延迟时间
                                    long bound = 5000L;
                                    // 观察到的最大事件时间
                                    // 为了防止溢出，初始值要加上bound+1L
                                    long maxTs = Long.MIN_VALUE + bound + 1L;
                                    @Override
                                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                        // 每来一条数据调用一次
                                        maxTs = Math.max(maxTs, event.f1);
//                                        output.emitWatermark(new Watermark(maxTs - bound - 1L));
                                        // 碰到key为hello的数据，发送一条水位线
                                        if (event.f0.equals("hello")) {
                                            output.emitWatermark(new Watermark(Long.MAX_VALUE));
                                        }
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        // 每隔200毫秒调用一次
                                        output.emitWatermark(new Watermark(maxTs - bound - 1L));
                                    }
                                };
                            }
                        }
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

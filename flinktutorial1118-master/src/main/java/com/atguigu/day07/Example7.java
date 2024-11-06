package com.atguigu.day07;

import com.atguigu.utils.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

// 基于间隔的join
// 业务1
// leftStream是订单表
// rightStream是订单详情表
// 业务2
// leftStream是购买事件
// rightStream是相同订单的物流事件
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> leftStream = env
                .fromElements(
                        new Event("key-1", "left", 10 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );

        SingleOutputStreamOperator<Event> rightStream = env
                .fromElements(
                        new Event("key-1", "right", 5 * 1000L),
                        new Event("key-1", "right", 6 * 1000L),
                        new Event("key-1", "right", 13 * 1000L),
                        new Event("key-1", "right", 18 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );

        leftStream.keyBy(r -> r.key)
                .intervalJoin(rightStream.keyBy(r -> r.key))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + " => " + right);
                    }
                })
                .print();

        env.execute();
    }
}

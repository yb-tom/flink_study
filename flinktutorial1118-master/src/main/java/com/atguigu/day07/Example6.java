package com.atguigu.day07;

import com.atguigu.utils.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

// 基于窗口的join
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> leftStream = env
                .fromElements(
                        new Event("key-1", "left", 1000L),
                        new Event("key-2", "left", 2000L),
                        new Event("key-1", "left", 3000L),
                        new Event("key-2", "left", 4000L)
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
                        new Event("key-1", "right", 1500L),
                        new Event("key-2", "right", 2500L),
                        new Event("key-1", "right", 3500L),
                        new Event("key-2", "right", 4500L)
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

        // 将来自两条流的相同key且属于相同窗口的数据进行join
        leftStream
                .join(rightStream)
                .where(r -> r.key)
                .equalTo(r -> r.key)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .apply(new JoinFunction<Event, Event, String>() {
                    @Override
                    public String join(Event first, Event second) throws Exception {
                        return first + " => " + second;
                    }
                })
                .print();

        env.execute();
    }
}

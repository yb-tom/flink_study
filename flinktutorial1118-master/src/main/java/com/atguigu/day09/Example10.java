package com.atguigu.day09;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment
                .create(env,
                        EnvironmentSettings.newInstance().inStreamingMode().build());

        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        $("key").as("userId"),
                        $("value").as("url"),
                        $("ts").rowtime(),
                        $("processingTs").proctime()
                );

        streamTableEnvironment.toChangelogStream(table).print();

        env.execute();
    }
}

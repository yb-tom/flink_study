package com.atguigu.day02;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// map举例
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .map(new MapFunction<Event, String>() {
                    @Override
                    public String map(Event value) throws Exception {
                        return value.key;
                    }
                })
                .print("使用匿名类实现map算子");

        env
                .addSource(new ClickSource())
                .map(new MyMap())
                .print("使用外部类实现map算子");

        env
                .addSource(new ClickSource())
                .map(r -> r.key)
                .print("使用匿名函数实现map算子");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<Event, String>() {
                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        out.collect(value.key);
                    }
                })
                .print("使用flatMap实现map");

        env.execute();
    }

    public static class MyMap implements MapFunction<Event, String> {
        @Override
        public String map(Event value) throws Exception {
            return value.key;
        }
    }
}

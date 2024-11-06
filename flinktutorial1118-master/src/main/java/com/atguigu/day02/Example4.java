package com.atguigu.day02;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// filter举例
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .filter(r -> r.key.equals("Mary"))
                .print("使用匿名函数实现filter算子");

        env
                .addSource(new ClickSource())
                .filter(new FilterFunction<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.key.equals("Mary");
                    }
                })
                .print("使用匿名类实现filter算子");

        env
                .addSource(new ClickSource())
                .filter(new MyFilter())
                .print("使用外部类实现filter");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<Event, Event>() {
                    @Override
                    public void flatMap(Event in, Collector<Event> out) throws Exception {
                        if (in.key.equals("Mary"))
                            out.collect(in);
                    }
                })
                .print("使用flatMap实现filter");

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event in) throws Exception {
            return in.key.equals("Mary");
        }
    }
}

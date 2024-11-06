package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// flatMap举例
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white", "black", "gray")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String in, Collector<String> out) throws Exception {
                        if (in.equals("white")) {
                            out.collect(in);
                        } else if (in.equals("black")) {
                            out.collect(in);
                            out.collect(in);
                        }
                    }
                })
                .print();

        env
                .fromElements("white", "black", "gray")
                .flatMap(new MyFlatMap())
                .print();

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String in, Collector<String> out) throws Exception {
            if (in.equals("white")) {
                out.collect(in);
            } else if (in.equals("black")) {
                out.collect(in);
                out.collect(in);
            }
        }
    }
}

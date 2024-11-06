package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 将所有算子的并行子任务的数量设置为1
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial1118/src/main/resources/words.txt")
                .flatMap(new Tokenizer()) // 外部类
                .keyBy(r -> r.f0) // f0字段设置为key
                .sum("f1")   // 聚合f1字段，底层逻辑就是Example1中的reduce算子
                .print();

        env.execute();
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = value.split(" ");
            for (String word : words)
                collector.collect(Tuple2.of(word, 1));
        }
    }
}
package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .map(new MapFunction<Integer, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> map(Integer in) throws Exception {
                        return Tuple3.of(in, in, in);
                    }
                })
                .keyBy(new KeySelector<Tuple3<Integer, Integer, Integer>, Integer>() {
                    @Override
                    public Integer getKey(Tuple3<Integer, Integer, Integer> value) throws Exception {
                        return 1;
                    }
                })
                .reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> in, Tuple3<Integer, Integer, Integer> acc) throws Exception {
                        return Tuple3.of(
                                Math.min(in.f0, acc.f0), // 最小值
                                Math.max(in.f1, acc.f1), // 最大值
                                in.f2 + acc.f2           // 总和
                        );
                    }
                })
                .print();

        env.execute();
    }
}

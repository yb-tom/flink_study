package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .map(r -> Tuple2.of(r, 1))
                // Tuple2<Object, Object>
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(r -> 1)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> in, Tuple2<Integer, Integer> acc) throws Exception {
                        return Tuple2.of(
                                in.f0 + acc.f0,
                                in.f1 + acc.f1
                        );
                    }
                })
                .map(new MapFunction<Tuple2<Integer, Integer>, String>() {
                    @Override
                    public String map(Tuple2<Integer, Integer> value) throws Exception {
                        return "平均值是：" + (value.f0 / value.f1);
                    }
                })
                .print();

        env.execute();
    }
}

package com.atguigu.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white", "black", "gray")
                .flatMap(
                        (String in, Collector<String> out) -> {
                            if (in.equals("white")) {
                                out.collect(in);
                            } else if (in.equals("black")) {
                                out.collect(in);
                                out.collect(in);
                            }
                        }
                )
                // lambda表达式在编译成字节码时，会发生类型擦除
                // Collector<String>被擦除成了Collector<Object>
                // 在运行时，jvm不知道Object的类型是什么
                // 所以需要做类型注解
                .returns(Types.STRING)
                .print();

        env.execute();
    }
}

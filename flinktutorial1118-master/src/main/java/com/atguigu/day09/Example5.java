package com.atguigu.day09;

import com.atguigu.utils.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .fromElements(
                        new Event("user-1", "fail", 1000L),
                        new Event("user-1", "fail", 2000L),
                        new Event("user-2", "success", 3000L),
                        new Event("user-1", "fail", 4000L),
                        new Event("user-1", "fail", 5000L)
                )
                // flink cep必须使用事件时间
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );

        stream
                .keyBy(r -> r.key)
                .process(new StateMachine())
                .print();

        env.execute();
    }

    public static class StateMachine extends KeyedProcessFunction<String, Event, String> {
        // 使用HashMap存储状态机
        private HashMap<Tuple2<String, String>, String> stateMachine = new HashMap<>();

        // 当前状态
        private ValueState<String> currentState;
        @Override
        public void open(Configuration parameters) throws Exception {
            // INITIAL状态接收到fail事件，跳转到S1状态
            // key: (当前状态，接收到的事件类型)
            // value: 将要跳转到的状态
            stateMachine.put(Tuple2.of("INITIAL", "fail"), "S1");
            stateMachine.put(Tuple2.of("INITIAL", "success"), "SUCCESS");
            stateMachine.put(Tuple2.of("S1", "fail"), "S2");
            stateMachine.put(Tuple2.of("S1", "success"), "SUCCESS");
            stateMachine.put(Tuple2.of("S2", "fail"), "FAIL");
            stateMachine.put(Tuple2.of("S2", "success"), "SUCCESS");

            currentState = getRuntimeContext().getState(
                    new ValueStateDescriptor<String>(
                            "current-state",
                            Types.STRING
                    )
            );
        }

        @Override
        public void processElement(Event in, Context ctx, Collector<String> out) throws Exception {
            // 第一条数据到达，先将当前状态置为初始状态
            if (currentState.value() == null) {
                currentState.update("INITIAL");
            }

            // 计算将要跳转到的状态
            String nextState = stateMachine.get(
                    Tuple2.of(currentState.value(), in.value)
            );

            if (nextState.equals("FAIL")) {
                out.collect("用户" + in.key + "连续三次登录失败");
                // 重置到S2状态
                currentState.update("S2");
            } else if (nextState.equals("SUCCESS")) {
                out.collect("用户" + in.key + "登录成功");
                // 重置到INITIAL状态
                currentState.update("INITIAL");
            } else {
                // 直接跳转到下一个状态
                currentState.update(nextState);
            }
        }
    }
}

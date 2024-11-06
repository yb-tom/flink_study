package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// KeyedProcessFunction维护的内部状态是：定时器
public class Example13 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .keyBy(r -> r)
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        // 获取processElement方法调用的机器时间
                        long currTs = ctx.timerService().currentProcessingTime();
                        // 获取30s之后的时间戳
                        long thirtySecondLater = currTs + 30 * 1000L;
                        // 注册30s之后的定时器
                        ctx.timerService().registerProcessingTimeTimer(thirtySecondLater);

                        // 获取60s之后的时间戳
                        long sixtySecondLater = currTs + 60 * 1000L;
                        // 注册60s之后的定时器
                        ctx.timerService().registerProcessingTimeTimer(sixtySecondLater);

                        out.collect("数据" + in + "到达，到达的机器时间是：" + new Timestamp(currTs) + "" +
                                "，注册的第一个定时器，时间戳是：" + new Timestamp(thirtySecondLater) + "" +
                                "，注册的第二个定时器，时间戳是：" + new Timestamp(sixtySecondLater));
                    }

                    // `timerTs`参数是上面注册的定时器的时间戳tenSecondLater
                    // 当机器时间超过`timerTs`，则触发onTimer的执行
                    @Override
                    public void onTimer(long timerTs, OnTimerContext ctx, Collector<String> out) throws Exception {
                        long currTs = ctx.timerService().currentProcessingTime();
                        out.collect("时间戳是" + new Timestamp(timerTs) + "的定时器触发了！onTimer执行的机器时间是：" +
                                "" + new Timestamp(currTs));
                    }
                })
                .print();

        env.execute();
    }
}

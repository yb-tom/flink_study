package com.atguigu.day04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class TimerServiceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collect("a");
                        Thread.sleep(30 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r)
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        // 获取当前机器时间
                        long currTs = ctx.timerService().currentProcessingTime();
                        long tenSecLater = currTs + 10000L;
                        // 注册了一个输入数据的key的定时器
                        // 将tenSecLater添加到数据的key所对应的定时器队列中
                        ctx.timerService().registerProcessingTimeTimer(tenSecLater);
                        out.collect("数据的key是：" + ctx.getCurrentKey() + "，注册的定时器是：" +
                                "" + new Timestamp(tenSecLater));
                    }

                    @Override
                    public void onTimer(long timerTs, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器的key：" + ctx.getCurrentKey() + "，定时器时间戳：" +
                                "" + new Timestamp(timerTs));
                    }
                })
                .print();

        env.execute();
    }
}

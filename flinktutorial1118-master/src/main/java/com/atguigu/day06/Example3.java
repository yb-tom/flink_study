package com.atguigu.day06;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        // 发送的数据是“hello”， 数据的事件时间是1000L
                        ctx.collectWithTimestamp("hello1", 1000L);
                        // 发送水位线事件
                        ctx.emitWatermark(new Watermark(999L));

                        ctx.collectWithTimestamp("hello2", 2000L);
                        ctx.emitWatermark(new Watermark(1999L));

                        ctx.collectWithTimestamp("hello3", 1500L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                            // ctx.timestamp()是输入数据in的事件时间戳
                            // 将迟到数据发送到侧输出流
                            ctx.output(
                                    // 侧输出流的名字，单例
                                    // 泛型是侧输出流中的元素类型
                                    new OutputTag<String>("late-event") {
                                    },
                                    "迟到的数据是：" + in + "，事件时间是：" +
                                            "" + ctx.timestamp() + "，当前process的水位线是：" +
                                            "" + ctx.timerService().currentWatermark()
                            );
                        } else {
                            out.collect("数据：" + in + " 没有迟到，事件时间是：" +
                                    "" + ctx.timestamp() + "，当前process的水位线是：" +
                                    "" + ctx.timerService().currentWatermark());
                        }
                    }
                });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("late-event"){}).print("侧输出流");

        env.execute();
    }
}

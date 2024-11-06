package com.atguigu.day09;

import com.atguigu.utils.Event;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

// 超时未支付订单的检测
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(
                                new Event(
                                        "order-1",
                                        "create",
                                        1000L
                                ),
                                1000L
                        );
                        ctx.collectWithTimestamp(
                                new Event(
                                        "order-2",
                                        "create",
                                        2000L
                                ),
                                2000L
                        );
                        ctx.collectWithTimestamp(
                                new Event(
                                        "order-1",
                                        "pay",
                                        3000L
                                ),
                                3000L
                        );
                    }

                    @Override
                    public void cancel() {

                    }
                });

        // 定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("create-order")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.value.equals("create");
                    }
                })
                // next表示紧挨着上一个事件
                .next("pay-order")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.value.equals("pay");
                    }
                })
                // 要求模板中的两个事件在5秒钟之内发生
                .within(Time.seconds(5));

        // 在数据流中匹配模板
        SingleOutputStreamOperator<String> result = CEP
                .pattern(stream.keyBy(r -> r.key), pattern)
                .flatSelect(
                        // 侧输出流用来接收订单超时信息
                        new OutputTag<String>("timeout-info") {
                        },
                        // 用来处理超时未支付的订单
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> map, long timeoutTimestamp, Collector<String> out) throws Exception {
                                // map {
                                //   "create-order": [Event]
                                // }
                                Event create = map.get("create-order").get(0);
                                // 将超时信息发送到侧输出流
                                out.collect("订单" + create.key + "超时未支付");
                            }
                        },
                        // 处理正常支付的订单
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> map, Collector<String> out) throws Exception {
                                // map {
                                //   "create-order": [Event],
                                //   "pay-order": [Event]
                                // }
                                Event create = map.get("create-order").get(0);
                                Event pay = map.get("pay-order").get(0);
                                // 向下游输出信息
                                out.collect("订单" + create.key + "正常支付，" +
                                        "创建订单的时间：" + create.ts + ";" +
                                        "支付订单的时间：" + pay.ts);
                            }
                        }
                );

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("timeout-info"){}).print("侧输出流");

        env.execute();
    }
}

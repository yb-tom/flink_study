package com.atguigu.day07;

import com.atguigu.utils.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

// 实时对账
// 业务
// leftStream: app支付事件流
// rightStream: weixin支付事件流
public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "left", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "left", 2000L), 2000L);
                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        DataStreamSource<Event> rightStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "right", 3000L), 3000L);
                        ctx.collectWithTimestamp(new Event("key-3", "right", 4000L), 4000L);
                        ctx.emitWatermark(new Watermark(7000));
                        Thread.sleep(1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "right", 200000 * 1000L), 20 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        leftStream.keyBy(r -> r.key)
                .connect(rightStream.keyBy(r -> r.key))
                .process(new Match())
                .print();

        env.execute();
    }

    public static class Match extends CoProcessFunction<Event, Event, String> {
        // 如果left事件先到达，将left事件保存到leftState中
        private ValueState<Event> leftState;
        // 如果right事件先到达，将right事件保存到rightState中
        private ValueState<Event> rightState;
        @Override
        public void open(Configuration parameters) throws Exception {
            leftState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>(
                            "left",
                            Types.POJO(Event.class)
                    )
            );
            rightState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>(
                            "right",
                            Types.POJO(Event.class)
                    )
            );
        }

        @Override
        public void processElement1(Event in1, Context ctx, Collector<String> out) throws Exception {
            // 如果left事件先到达，那么rightState必定为空
            if (rightState.value() == null) {
                // 把left事件保存下来
                leftState.update(in1);
                // 注册一个5秒钟之后的定时器
                ctx.timerService().registerEventTimeTimer(in1.ts + 5000L);
            }
            // 如果left事件后到达，那么rightState不为空
            // 对账成功
            else {
                out.collect(in1.key + "对账成功，right事件先到达。");
                // 清空rightState
                rightState.clear();
            }
        }

        @Override
        public void processElement2(Event in2, Context ctx, Collector<String> out) throws Exception {
            if (leftState.value() == null) {
                rightState.update(in2);
                ctx.timerService().registerEventTimeTimer(in2.ts + 5000L);
            } else {
                out.collect(in2.key + "对账成功，left事件先到达。");
                leftState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (leftState.value() != null) {
                out.collect(leftState.value().key + "对账失败，right事件没来");
                leftState.clear();
            }
            if (rightState.value() != null) {
                out.collect(rightState.value().key + "对账失败，left事件没来");
                rightState.clear();
            }
        }
    }
}

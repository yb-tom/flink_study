package com.atguigu.day06;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r -> r.key)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .trigger(new Trigger<Event, TimeWindow>() {
                    // 每来一条数据调用一次
                    // 当窗口的第一条数据到达以后，以第一条数据的事件时间为基准
                    // 在接下来的每个整数秒，都触发一次窗口计算
                    // 比如事件时间是1234毫秒，那么在2000ms、3000ms、...触发窗口计算
                    @Override
                    public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        // 初始化一个标志位
                        // 每个窗口独有的状态变量
                        ValueState<Boolean> flag = ctx.getPartitionedState(
                                new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN)
                        );

                        // 窗口第一条数据到来才会进入if里面的逻辑
                        if (flag.value() == null) {
                            // 将标志位置为true，这样后面的数据过来
                            // 就不会调用if里面的逻辑了
                            flag.update(true);
                            long currTs = element.ts;
                            // 根据currTs计算出紧挨着的接下来的整数秒
                            // 1234 + 1000 - 1234 % 1000 => 2000
                            long nextSecond = currTs + 1000L - currTs % 1000L;
                            // 注册定时器（onEventTime）
                            ctx.registerEventTimeTimer(nextSecond);
                        }

                        return TriggerResult.CONTINUE;
                    }

                    // 机器时间到达`time`参数，触发调用
                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    // 水位线超过`timerTs`参数，触发调用
                    @Override
                    public TriggerResult onEventTime(long timerTs, TimeWindow window, TriggerContext ctx) throws Exception {
                        if (timerTs < window.getEnd()) {
                            if (timerTs + 1000L < window.getEnd()) {
                                // 如果timerTs + 1000L还是小于窗口结束时间
                                // 那么再注册一个定时器（onEventTime）
                                ctx.registerEventTimeTimer(timerTs + 1000L);
                            }
                            // 触发trigger后面的process算子的执行
                            return TriggerResult.FIRE;
                        }
                        return TriggerResult.CONTINUE;
                    }

                    // 窗口闭合时调用
                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        ValueState<Boolean> flag = ctx.getPartitionedState(
                                new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN)
                        );
                        flag.clear();
                    }
                })
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        out.collect("用户：" + key + " 在窗口" +
                                "" + new Timestamp(context.window().getStart()) + "~" +
                                "" + new Timestamp(context.window().getEnd()) + "浏览次数是：" +
                                "" + elements.spliterator().getExactSizeIfKnown());
                    }
                })
                .print();

        env.execute();
    }
}

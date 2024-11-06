package com.atguigu.day05;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import com.atguigu.utils.UserViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

// Example1的底层实现
public class WindowFuncCusT {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.key)
                .process(new MyTumblingProcessingTimeWindow(5000L))
                .print();

        env.execute();
    }

    public static class MyTumblingProcessingTimeWindow extends KeyedProcessFunction<String, Event, UserViewCountPerWindow> {
        private long windowSize;

        public MyTumblingProcessingTimeWindow(long windowSize) {
            this.windowSize = windowSize;
        }

        // key: (windowStartTime, windowEndTime)
        // value: 窗口中所有数据组成的List
        private MapState<Tuple2<Long, Long>, List<Event>> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Tuple2<Long, Long>, List<Event>>(
                            "windowinfo-eventlist",
                            Types.TUPLE(Types.LONG, Types.LONG),
                            Types.LIST(Types.POJO(Event.class))
                    )
            );
        }

        @Override
        public void processElement(Event in, Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 每来一条数据，就将数据添加到所属窗口
            // 数据in属于哪一个窗口？
            long currTs = ctx.timerService().currentProcessingTime();
            long windowStartTime = currTs - currTs % windowSize;
            long windowEndTime = windowStartTime + windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            if (!mapState.contains(windowInfo)) {
                // 如果mapState不包含windowInfo这个key
                // 说明输入数据in是这个窗口的第一条数据

                // 从下面的代码中可以看出，flink是窗口的第一条数据到达，才会开窗口
                ArrayList<Event> windowElements = new ArrayList<>();
                windowElements.add(in);
                mapState.put(windowInfo, windowElements);
            } else {
                // 如果窗口已经存在，则直接将数据in添加到窗口对应的ArrayList中
                mapState.get(windowInfo).add(in);
            }

            // 注册一个`窗口结束时间-1毫秒`的定时器
            // 来的数据是1234毫秒，那么将会注册一个4999毫秒的定时器
            // 又来了一条2234毫秒的数据呢？注册的还是4999毫秒的定时器，不起作用
            ctx.timerService().registerProcessingTimeTimer(
                    windowEndTime - 1L
            );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 使用timestamp+1反向计算出窗口结束时间
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            String username = ctx.getCurrentKey();
            // 获取窗口中元素的数量，也就是username在这个窗口的浏览次数
            int count = mapState.get(windowInfo).size();
            out.collect(new UserViewCountPerWindow(
                    username,
                    count,
                    windowStartTime,
                    windowEndTime
            ));
            // 销毁这个窗口
            mapState.remove(windowInfo);
        }
    }
}

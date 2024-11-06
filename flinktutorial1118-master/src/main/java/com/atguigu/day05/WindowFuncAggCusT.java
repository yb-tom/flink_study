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

// Example2的底层实现
public class WindowFuncAggCusT {
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
        // value: 窗口的累加器
        private MapState<Tuple2<Long, Long>, Integer> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Tuple2<Long, Long>, Integer>(
                            "windowInfo-windowAcc",
                            Types.TUPLE(Types.LONG, Types.LONG),
                            Types.INT
                    )
            );
        }

        @Override
        public void processElement(Event in, Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            long currTs = ctx.timerService().currentProcessingTime();
            long windowStartTime = currTs - currTs % windowSize;
            long windowEndTime = windowStartTime + windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            if (!mapState.contains(windowInfo)) {
                mapState.put(windowInfo, 1);
            } else {
                mapState.put(windowInfo, mapState.get(windowInfo) + 1);
            }

            ctx.timerService().registerProcessingTimeTimer(
                    windowEndTime - 1L
            );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            int count = mapState.get(windowInfo);
            String username = ctx.getCurrentKey();
            out.collect(new UserViewCountPerWindow(
                    username,count,windowStartTime,windowEndTime
            ));
            mapState.remove(windowInfo);
        }
    }
}

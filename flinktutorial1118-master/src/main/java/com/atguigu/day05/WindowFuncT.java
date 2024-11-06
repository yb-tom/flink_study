package com.atguigu.day05;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import com.atguigu.utils.UserViewCountPerWindow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// 计算每个用户在每个5秒钟滚动窗口中的浏览次数
public class WindowFuncT {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Event, UserViewCountPerWindow, String, TimeWindow> {
        // 窗口闭合的时候调用
        // 时间超过`窗口结束时间-1毫秒`时，触发调用
        @Override
        public void process(String username, Context context, Iterable<Event> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            // Iterable<Event> elements中包含了窗口中的所有元素
            int count = 0;
            for (Event e : elements) count += 1;
            long windowStartTime = context.window().getStart();
            long windowEndTime = context.window().getEnd();
            out.collect(new UserViewCountPerWindow(
                    username,
                    count,
                    windowStartTime,
                    windowEndTime
            ));
        }
    }
}

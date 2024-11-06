package com.atguigu.day05;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import com.atguigu.utils.UserViewCountPerWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowFuncAccT {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.key)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<Event, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Event value, Integer accumulator) {
            return accumulator + 1;
        }

        // 窗口闭合时调用，将返回值发送给ProcessWindowFunction
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    // 注意！！！输入泛型是getResult发送过来的数据类型，也就是Integer
    public static class WindowResult extends ProcessWindowFunction<Integer, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Integer> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            // 迭代器Iterable<Integer> elements中只有一个元素！！！
            // 就是getResult的返回值
            out.collect(new UserViewCountPerWindow(
                    key,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }
}

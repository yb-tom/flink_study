package com.atguigu.day07;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collect("a");
                        Thread.sleep(1000L);
                        ctx.collect("a");
                        Thread.sleep(6000L);
                        ctx.collect("a");
                        Thread.sleep(6000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> 1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        out.collect("窗口" + new Timestamp(context.window().getStart()) + "~" +
                                "" + new Timestamp(context.window().getEnd()) + "里面有" +
                                "" + elements.spliterator().getExactSizeIfKnown() + "条数据");
                    }
                })
                .print();

        env.execute();
    }
}

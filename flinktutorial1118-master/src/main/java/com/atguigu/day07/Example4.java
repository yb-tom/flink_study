package com.atguigu.day07;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

// 查询流
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());

        DataStreamSource<String> queryStream = env.socketTextStream("localhost", 9999);

        clickStream.keyBy(r -> r.key)
                .connect(
                        // 广播之前必须将并行度设置为1
                        queryStream.setParallelism(1).broadcast()
                )
                .flatMap(new CoFlatMapFunction<Event, String, Event>() {
                    private String queryString = "";
                    @Override
                    public void flatMap1(Event in, Collector<Event> out) throws Exception {
                        if (in.value.equals(queryString)) out.collect(in);
                    }

                    @Override
                    public void flatMap2(String in, Collector<Event> out) throws Exception {
                        queryString = in;
                    }
                })
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute();
    }
}

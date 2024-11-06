package com.atguigu.day07;

import com.atguigu.utils.Event;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

// select * from A inner join B on A.key=B.key;
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .fromElements(
                        new Event("key-1", "left", 1000L),
                        new Event("key-2", "left", 2000L),
                        new Event("key-1", "left", 3000L),
                        new Event("key-2", "left", 4000L)
                );

        DataStreamSource<Event> rightStream = env
                .fromElements(
                        new Event("key-1", "right", 1500L),
                        new Event("key-2", "right", 2500L),
                        new Event("key-1", "right", 3500L),
                        new Event("key-2", "right", 4500L)
                );

        leftStream.keyBy(r -> r.key)
                .connect(rightStream.keyBy(r -> r.key))
                .process(new InnerJoin())
                .print();

        env.execute();
    }

    public static class InnerJoin extends CoProcessFunction<Event, Event, String> {
        private ListState<Event> leftHistory;
        private ListState<Event> rightHistory;
        @Override
        public void open(Configuration parameters) throws Exception {
            leftHistory = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>("left-history", Types.POJO(Event.class))
            );
            rightHistory = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>("right-history", Types.POJO(Event.class))
            );
        }

        @Override
        public void processElement1(Event in1, Context ctx, Collector<String> out) throws Exception {
            leftHistory.add(in1);

            // 第一条流每来一条数据，就和第二条流的所有的历史数据进行join
            for (Event right : rightHistory.get()) {
                out.collect(in1 + " => " + right);
            }
        }

        @Override
        public void processElement2(Event in2, Context ctx, Collector<String> out) throws Exception {
            rightHistory.add(in2);

            // 第二条流每来一条数据，就和第一条流的所有的历史数据进行join
            for (Event left : leftHistory.get()) {
                out.collect(left + " => " + in2);
            }
        }
    }
}

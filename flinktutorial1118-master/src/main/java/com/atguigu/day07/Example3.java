package com.atguigu.day07;

import com.atguigu.utils.ProductViewPerWindow;
import com.atguigu.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        env
                .addSource(new FlinkKafkaConsumer<String>(
                        "userbehavior-1118",
                        new SimpleStringSchema(),
                        properties
                ))
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String in) throws Exception {
                        String[] array = in.split(",");
                        return new UserBehavior(
                                array[0], array[1], array[2], array[3],
                                Long.parseLong(array[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(UserBehavior value, Long accumulator) {
                                return accumulator + 1L;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, ProductViewPerWindow, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<ProductViewPerWindow> out) throws Exception {
                                out.collect(new ProductViewPerWindow(
                                        s,
                                        elements.iterator().next(),
                                        context.window().getStart(),
                                        context.window().getEnd()
                                ));
                            }
                        }
                )
                .keyBy(r -> r.windowEndTime)
                .process(new KeyedProcessFunction<Long, ProductViewPerWindow, String>() {
                    private ListState<ProductViewPerWindow> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<ProductViewPerWindow>(
                                        "list-state",
                                        Types.POJO(ProductViewPerWindow.class)
                                )
                        );
                    }

                    @Override
                    public void processElement(ProductViewPerWindow in, Context ctx, Collector<String> out) throws Exception {
                        listState.add(in);
                        ctx.timerService().registerEventTimeTimer(in.windowEndTime);
                    }

                    @Override
                    public void onTimer(long timerTs, OnTimerContext ctx, Collector<String> out) throws Exception {
                        ArrayList<ProductViewPerWindow> arrayList = new ArrayList<>();
                        for (ProductViewPerWindow e : listState.get()) arrayList.add(e);
                        listState.clear();

                        arrayList.sort(new Comparator<ProductViewPerWindow>() {
                            @Override
                            public int compare(ProductViewPerWindow t1, ProductViewPerWindow t2) {
                                return (int) (t2.count - t1.count);
                            }
                        });
                        StringBuilder result = new StringBuilder();
                        result.append("====================================\n");
                        result.append("窗口结束时间：" + new Timestamp(timerTs) + "\n");
                        for (int i = 0; i < 3; i++) {
                            ProductViewPerWindow t = arrayList.get(i);
                            result.append("第" + (i+1) + "名的商品ID是：" + t.productId + ", " +
                                    "浏览次数是：" + t.count + "\n");
                        }
                        result.append("====================================\n");
                        out.collect(result.toString());
                    }
                })
                .print();

        env.execute();
    }
}

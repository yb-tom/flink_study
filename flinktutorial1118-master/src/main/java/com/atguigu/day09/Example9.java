package com.atguigu.day09;

import com.atguigu.utils.ProductViewPerWindow;
import com.atguigu.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

// Example8的底层实现
public class Example9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial1118/src/main/resources/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                        String[] arr = value.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                        if (userBehavior.type.equals("pv"))
                            out.collect(userBehavior);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                // 以下三句对应sql：
                // select productId, count(productId) from userbehavior
                // group by productId, HOP(...)
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .process(new ProcessWindowFunction<UserBehavior, ProductViewPerWindow, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<ProductViewPerWindow> out) throws Exception {
                        out.collect(new ProductViewPerWindow(
                                s,
                                elements.spliterator().getExactSizeIfKnown(),
                                context.window().getStart(),
                                context.window().getEnd()
                        ));
                    }
                })
                .keyBy(r -> r.windowEndTime)
                .process(new KeyedProcessFunction<Long, ProductViewPerWindow, String>() {
                    private ListState<ProductViewPerWindow> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<ProductViewPerWindow>(
                                        "list",
                                        Types.POJO(ProductViewPerWindow.class)
                                )
                        );
                    }

                    @Override
                    public void processElement(ProductViewPerWindow value, Context ctx, Collector<String> out) throws Exception {
                        listState.add(value);
                        ArrayList<ProductViewPerWindow> arrayList = new ArrayList<>();
                        for (ProductViewPerWindow e : listState.get()) arrayList.add(e);

                        arrayList.sort(new Comparator<ProductViewPerWindow>() {
                            @Override
                            public int compare(ProductViewPerWindow t1, ProductViewPerWindow t2) {
                                return (int) (t2.count - t1.count);
                            }
                        });

                        StringBuilder result = new StringBuilder();
                        result.append("窗口结束时间：" + new Timestamp(value.windowEndTime) + "\n");
                        if (arrayList.size() >= 3) {
                            for (int i = 0; i < 3; i++) {
                                ProductViewPerWindow tmp = arrayList.get(i);
                                result.append("第" + (i+1) + "名，商品ID：" + tmp.productId + "," +
                                        "浏览次数：" + tmp.count + "\n");
                            }
                        } else {
                            for (int i = 0; i < arrayList.size(); i++) {
                                ProductViewPerWindow tmp = arrayList.get(i);
                                result.append("第" + (i+1) + "名，商品ID：" + tmp.productId + "," +
                                        "浏览次数：" + tmp.count + "\n");
                            }
                        }
                        out.collect(result.toString());
                    }
                })
                .print();

        env.execute();
    }
}

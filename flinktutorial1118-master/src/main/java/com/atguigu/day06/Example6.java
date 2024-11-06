package com.atguigu.day06;

import com.atguigu.utils.ProductViewPerWindow;
import com.atguigu.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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

// 实时热门商品
// 每隔5分钟计算一次过去1小时浏览次数最多的3个商品ID
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial1118/src/main/resources/UserBehavior.csv")
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
                        // 设置最大延迟时间为0
                        // 相当于forBoundedOutOfOrderness(Duration.ofSeconds(0))
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
                // aggregate计算出的是ProductViewPerWindow
                .aggregate(new CountAgg(), new WindowResult())
                // 接下来应该将属于同一个窗口的ProductViewPerWindow分到一个组里
                // 然后根据ProductViewPerWindow中的count字段，进行降序排列
                .keyBy(r -> r.windowEndTime)
                // 首先将每个组的ProductViewPerWindow保存到ListState中
                // 然后等组里的ProductViewPerWindow全到达之后，按照count降序排列
                .process(new TopN(3))
                .print();

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, ProductViewPerWindow, String> {
        private int n;

        public TopN(int n) {
            this.n = n;
        }

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

            // 注册定时器
            // 在水位线超过in.windowEndTime + 1L时，
            // in.windowEndTime所对应的所有的ProductViewPerWindow数据都已经到达
            ctx.timerService().registerEventTimeTimer(
                    in.windowEndTime + 1L
            );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发时，就是降序排列的时候
            ArrayList<ProductViewPerWindow> arrayList = new ArrayList<>();
            for (ProductViewPerWindow e : listState.get())
                arrayList.add(e);
            // 由于listState中的数据都已经取出，且排序之后listState就没用了
            // 所以清除掉listState，节省内存
            listState.clear();

            // 降序排列
            arrayList.sort(new Comparator<ProductViewPerWindow>() {
                @Override
                public int compare(ProductViewPerWindow t1, ProductViewPerWindow t2) {
                    return (int) (t2.count - t1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("=========================================\n");
            result.append("窗口结束时间：" + new Timestamp(timestamp - 1L) + "\n");
            for (int i = 0; i < n; i++) {
                ProductViewPerWindow tmp = arrayList.get(i);
                result.append("第" + (i+1) + "名的商品ID是：" + tmp.productId + "，" +
                        "浏览次数是：" + tmp.count + "\n");
            }
            result.append("=========================================\n");
            out.collect(result.toString());
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, ProductViewPerWindow, String, TimeWindow> {
        @Override
        public void process(String productId, Context context, Iterable<Long> elements, Collector<ProductViewPerWindow> out) throws Exception {
            out.collect(new ProductViewPerWindow(
                    productId,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
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
    }
}

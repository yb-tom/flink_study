package com.atguigu.day09;

import com.atguigu.utils.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("/home/zuoyuan/flinktutorial1118/src/main/resources/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] arr = in.split(",");
                        UserBehavior userBehavior = new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
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
                );

        // 获取表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment
                .create(
                        env,
                        EnvironmentSettings.newInstance().inStreamingMode().build()
                );

        // 将数据流转换成动态表
        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        $("userId"),
                        $("productId").as("pid"),
                        $("categoryId").as("cid"),
                        $("type"),
                        $("ts").rowtime() // rowtime表示这一列是事件时间
                );

        // 将动态表注册为临时视图
        streamTableEnvironment.createTemporaryView("userbehavior", table);

        // 查询
        // ProductViewPerWindow
        // keyBy(r -> r.productId).window(SlidingEventTimeWindows.of(...))
        // .aggregate(new CountAgg, new WindowResult)
        Table result = streamTableEnvironment
                // HOP(时间戳的列名，滑动距离，窗口长度)
                // COUNT的底层只使用了ProcessWindowFunction，将窗口中的数据都保存下来然后计数
                .sqlQuery(
                        "SELECT pid, COUNT(pid) as cnt, " +
                                "HOP_START(ts, INTERVAL '5' MINUTES, INTERVAL '1' HOURS) as windowStartTime, " +
                                "HOP_END(ts, INTERVAL '5' MINUTES, INTERVAL '1' HOURS) as windowEndTime " +
                                "FROM userbehavior GROUP BY " +
                                "pid," +
                                "HOP(ts, INTERVAL '5' MINUTES, INTERVAL '1' HOURS)"
                );

        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();
    }
}

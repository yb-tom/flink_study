package com.atguigu.day04;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.key)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    // key: url
                    // value: url的访问次数
                    private MapState<String, Integer> urlCount;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        urlCount = getRuntimeContext().getMapState(
                                new MapStateDescriptor<String, Integer>(
                                        "url-count",
                                        Types.STRING,
                                        Types.INT
                                )
                        );
                    }

                    @Override
                    public void processElement(Event in, Context ctx, Collector<String> out) throws Exception {
                        if (!urlCount.contains(in.value)) {
                            // 如果字典状态变量中没有in.value这个url，那么说明是in.key这个用户第一次访问这个url
                            urlCount.put(in.value, 1);
                        } else {
                            // 如果ctx.getCurrentKey()之前访问过url的话，那么访问次数+1
                            urlCount.put(in.value, urlCount.get(in.value) + 1);
                        }

                        // 格式化字符串输出
                        String username = ctx.getCurrentKey();
                        StringBuilder result = new StringBuilder();
                        result.append(username + " => {\n");
                        // 遍历当前用户的字典状态变量中所有的key，也就是所有的url
                        for (String url : urlCount.keys()) {
                            result.append("  " + url + " -> " + urlCount.get(url));
                            result.append("\n");
                        }
                        result.append("}\n");

                        // 输出数据
                        out.collect(result.toString());
                    }
                })
                .print();

        env.execute();
    }
}

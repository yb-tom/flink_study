package com.atguigu.day04;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(10000));
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(r -> r % 2)
                .process(new KeyedProcessFunction<Integer, Integer, String>() {
                    private ListState<Integer> history;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        history = getRuntimeContext().getListState(
                                new ListStateDescriptor<Integer>(
                                        "history",
                                        Types.INT
                                )
                        );
                    }

                    @Override
                    public void processElement(Integer in, Context ctx, Collector<String> out) throws Exception {
                        // 每来一条数据就添加到列表状态变量中
                        // 根据in的key将in添加到key对应的列表状态变量中
                        // 例如in是偶数，那么将in添加到key为0对应的ListState中
                        history.add(in);

                        // 将key对应的ListState中的数据取出，并放入ArrayList中进行排序
                        ArrayList<Integer> arrayList = new ArrayList<>();
                        // history.get()将in的key所对应的ListState中的元素取出
                        for (Integer i : history.get()) arrayList.add(i);

                        // 升序排列
                        arrayList.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer i1, Integer i2) {
                                return i1 - i2;
                            }
                        });

                        String result = "";
                        if (ctx.getCurrentKey() == 0) {
                            result += "偶数：";
                        } else {
                            result += "奇数：";
                        }

                        for (Integer i : arrayList) {
                            result += (i + "  ");
                        }

                        out.collect(result);
                    }
                })
                .print();

        env.execute();
    }
}

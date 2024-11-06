package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

// 自定义并行数据源
public class RichParallelSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new RichParallelSourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        for (int i = 1; i < 5; i++) {
                            int idx = getRuntimeContext().getIndexOfThisSubtask();
                            ctx.collect("source的并行子任务：" + idx + " 发送了数据：" + i);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .print().setParallelism(2);

        env.execute();
    }
}

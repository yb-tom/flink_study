package com.atguigu.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

// 检测温度连续1秒钟上升
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensorId)
                .process(new TempAlert())
                .print();

        env.execute();
    }

    // 如果温度上升，那么注册1秒钟之后的报警定时器
    // 如果1秒钟之内出现温度下降，则删除报警定时器
    public static class TempAlert extends KeyedProcessFunction<String, SensorReading, String> {
        private ValueState<Double> lastTemp; // 用来保存上一次的温度值
        private ValueState<Long> timerTs; // 用来保存注册的报警定时器的时间戳
        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>(
                            "last-temp",
                            Types.DOUBLE
                    )
            );
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>(
                            "timer-ts",
                            Types.LONG
                    )
            );
        }

        // 一秒钟之内：1.1, 2.2, 3.3, 2.2, 4.4
        @Override
        public void processElement(SensorReading in, Context ctx, Collector<String> out) throws Exception {
            // 将上一次温度保存到一个临时变量中
            // 如果输入数据是第一条温度，那么prevTemp将是null
            Double prevTemp = lastTemp.value();
            // 将本次温度保存到状态变量中
            lastTemp.update(in.temperature);

            // 取出报警定时器的时间戳
            // 如果没有注册过报警定时器，那么ts将是null
            Long ts = timerTs.value();

            if (prevTemp != null) {
                // 温度上升且没有注册过报警定时器
                if (in.temperature > prevTemp && ts == null) {
                    // 注册1秒钟之后的报警定时器
                    long oneSecLater = ctx.timerService().currentProcessingTime() + 1000L;
                    ctx.timerService().registerProcessingTimeTimer(oneSecLater);
                    // 将报警定时器的时间戳保存在状态变量timerTs中
                    timerTs.update(oneSecLater);
                }
                // 1秒钟之内温度下降且存在报警定时器
                else if (in.temperature < prevTemp && ts != null) {
                    // 手动将定时器从定时器队列中删除
                    ctx.timerService().deleteProcessingTimeTimer(ts);
                    // 将timerTs清空，表示不存在报警定时器了
                    timerTs.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器" + ctx.getCurrentKey() + "连续1秒钟温度上升！");
            // 报警完毕之后，由于onTimer执行完之后，定时器时间戳从定时器队列中弹出，所以报警定时器不存在了
            // 需要将timerTs清空
            timerTs.clear();
        }
    }

    public static class SensorReading {
        public String sensorId;
        public Double temperature;

        public SensorReading() {
        }

        public SensorReading(String sensorId, Double temperature) {
            this.sensorId = sensorId;
            this.temperature = temperature;
        }
    }

    public static class SensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private Random random = new Random();
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running) {
                for (int i = 1; i < 4; i++) {
                    ctx.collect(new SensorReading(
                            "sensor_" + i,
                            random.nextGaussian()
                    ));
                }
                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

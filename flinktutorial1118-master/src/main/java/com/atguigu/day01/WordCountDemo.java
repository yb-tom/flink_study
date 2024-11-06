package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 单词计数
public class WordCountDemo {
    // 不要忘记抛出异常！
    public static void main(String[] args) throws Exception {
        // 获取执行环境上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                // 从socket读取数据
                // 先启动`nc -lk 9999`
                .socketTextStream("localhost", 9999)
                // 将数据源算子的并行子任务的数量设置为1
                .setParallelism(1)
                // map操作
                // 将文本使用空格进行切割，然后转换成元组
                // "hello world" => ("hello", 1), ("world", 1)
                // 使用匿名类的方式实现flatMap算子的计算逻辑
                // 第一个泛型：flatMap输入的泛型，也就是socketTextStream算子输出的数据类型
                // 第二个泛型：flatMap输出的泛型
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    // 输入数据到达，触发flatMap方法的执行
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 第一个参数：输入数据
                        // 第二个参数：集合，集合里面的数据类型是元组
                        // 集合收集将要向下游发送的数据，然后由flink引擎将集合中的数据发送出去

                        // 切割字符串
                        String[] words = value.split(" ");
                        // 转换成元组，并收集到集合当中
                        for (String word : words) {
                            // .collect方法收集将要发送的数据
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                // 将flatMap的并行子任务的数量设置为1
                .setParallelism(1)
                // shuffle阶段
                // 将元组按照不同的key进行分组
                // 使用匿名类实现
                // 第一个泛型：输入数据的泛型
                // 第二个泛型：key的泛型
                // 为输入数据指定key
                // 将数据路由到key对应的逻辑分区
                // 由于keyBy不负责计算，所以不能设置并行子任务的数量
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        // 将f0字段指定为元组的key
                        // f0相当于scala元组的`_1`
                        return value.f0;
                    }
                })
                // reduce阶段
                // reduce算子会为每个key维护一个累加器
                // reduce算子的输出是累加器
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    // reduce方法定义了输入数据和累加器的聚合规则
                    // 两个参数：一个是输入数据，一个是累加器
                    // 返回值是新的累加器，覆盖掉旧的累加器
                    // 累加器最开始为空
                    // reduce方法执行完，更新完累加器之后，输入数据被丢弃
                    // 所以reduce只会为每个key维护一个累加器，并不保存输入数据
                    // 输入数据更新完累加器以后，输入数据被丢弃
                    // 所以非常节省内存
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                // reduce的并行子任务的数量设置为1
                .setParallelism(1)
                // 输出
                .print()
                // print的并行子任务的数量设置为1
                .setParallelism(1);

        // 提交并执行任务
        env.execute();
    }
}

package com.atguigu.day08;

import com.atguigu.utils.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // flink默认保存最近一次的检查点
        // 每隔10秒钟保存一次检查点
        env.enableCheckpointing(10 * 1000L);
        // 设置检查点文件的保存路径
        // file:// + 检查点文件夹的路径
        // windows中要注意反斜杠的问题
        env.setStateBackend(new FsStateBackend("file:///home/zuoyuan/flinktutorial1118/src/main/resources/ckpts"));

        env.addSource(new ClickSource()).print();

        env.execute();
    }
}

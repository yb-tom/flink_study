package com.atguigu.day08;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10 * 1000L);

        env
                .addSource(new CounterSource())
                .addSink(new TransactionalFileSink());

        env.execute();
    }

    // 模拟一个可重置偏移量的数据源
    public static class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {
        private boolean running = true;
        private Long offset = 0L;

        // 算子状态，仅对当前并行子任务可见
        private ListState<Long> state;

        // 程序启动时触发调用（第一次启动，故障恢复启动）
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 获取算子状态，如果是第一次启动程序，则初始化新的
            // 如果是故障恢复，则从检查点文件中读取
            state = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>(
                            "state",
                            LongSerializer.INSTANCE
                    )
            );

            // 获取检查点文件中的最近一次的刚消费完的偏移量
            // 如果程序是第一次启动的话
            // 那么state.get()迭代器中没有元素，循环不执行
            // 如果是故障恢复，那么读取检查点文件中的刚消费完的偏移量
            for (Long l : state.get()) {
                // 取出列表状态变量中的最后一个偏移量
                offset = l;
            }
        }

        // 检查点分界线进入source的并行子任务就会触发调用
        // 对算子状态做快照
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear(); // 清空算子状态
            state.add(offset); // 将刚消费完的偏移量保存下来
        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            final Object lock = ctx.getCheckpointLock();
            while (running) {
                // 发送的数据和偏移量相等
                synchronized (lock) {
                    // 保证以下两条语句不被打断，也就是原子性执行
                    ctx.collect(offset);
                    offset += 1;
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // 输入数据的类型是Long
    // 事务的类型是一个字符串（文件名）
    public static class TransactionalFileSink extends TwoPhaseCommitSinkFunction<Long, String, Void> {
        private BufferedWriter transactionWriter;

        public TransactionalFileSink() {
            super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
        }

        // 当开启事务的时候触发调用（数据源发送第一条数据/每个事务正式提交以后开启新的事务）
        @Override
        protected String beginTransaction() throws Exception {
            System.out.println("开启事务");
            long timeNow = System.currentTimeMillis();
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String transactionFile = timeNow + "-" + taskIdx; // 独一无二的文件名
            Path tFilePath = Paths.get("/home/zuoyuan/filetemp/" + transactionFile);
            Files.createFile(tFilePath); // 创建临时文件
            this.transactionWriter = Files.newBufferedWriter(tFilePath);
            return transactionFile; // 将文件名返回，其它方法也可以访问
        }

        // 每来一条数据调用一次
        @Override
        protected void invoke(String transaction, Long value, Context context) throws Exception {
            transactionWriter.write(value.toString());
            transactionWriter.write("\n");
        }

        // 预提交时触发调用，周期性的调用
        @Override
        protected void preCommit(String transaction) throws Exception {
            transactionWriter.flush();
            transactionWriter.close();
        }

        // 正式提交时触发调用
        // 实际上就是sink算子接收到作业管理器广播出来的检查点完成的通知时
        // 触发commit调用
        @Override
        protected void commit(String transaction) {
            Path tFilePath = Paths.get("/home/zuoyuan/filetemp/" + transaction);
            if (Files.exists(tFilePath)) {
                try {
                    Path cFilePath = Paths.get("/home/zuoyuan/filetarget/" + transaction);
                    // 将临时文件名改成正式文件名
                    Files.move(tFilePath, cFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // 程序宕机回滚事务时触发调用
        // 回滚操作：删除临时文件
        @Override
        protected void abort(String transaction) {
            Path tFilePath = Paths.get("/home/zuoyuan/filetemp/" + transaction);
            if (Files.exists(tFilePath)) {
                try {
                    Files.delete(tFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

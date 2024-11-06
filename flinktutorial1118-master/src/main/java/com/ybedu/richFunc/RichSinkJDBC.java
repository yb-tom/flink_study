package com.ybedu.richFunc;

import com.atguigu.utils.ClickSource;
import com.atguigu.utils.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

// create database userbehavior;
// use userbehavior;
// create table clicks (username varchar(100), url varchar(100));
public class RichSinkJDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .addSink(new MyJDBC());

        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<Event> {
        private Connection connection;
        private PreparedStatement insertStmt; // 插入语句
        private PreparedStatement updateStmt; // 更新语句
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/userbehavior?useSSL=false",
                    "root",
                    "root"
            );
            // 插入语句
            insertStmt = connection.prepareStatement("insert into clicks (username, url) values (?,?)");
            // 更新语句
            updateStmt = connection.prepareStatement("update clicks set url = ? where username = ?");
        }

        // 每来一条数据，触发一次调用
        // 幂等性地写入MySQL
        // 如果某个username的数据已经存在，则覆盖
        // 如果username不存在，则插入数据
        @Override
        public void invoke(Event in, Context context) throws Exception {
            // 执行更新语句
            updateStmt.setString(1, in.value);
            updateStmt.setString(2, in.key);
            updateStmt.execute();

            // 如果为0,则说明表中没有username对应的行
            // 那么插入数据
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, in.key);
                insertStmt.setString(2, in.value);
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}

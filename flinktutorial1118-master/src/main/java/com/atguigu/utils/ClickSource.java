package com.atguigu.utils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

// 点击流的自定义数据源
public class ClickSource implements SourceFunction<Event> {
    private boolean running = true;
    private Random random = new Random();
    private String[] userArray = {"Mary", "Bob", "Alice"};
    private String[] urlArray = {"./home", "./cart", "./buy"};
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 一直不停地的发送数据
        while (running) {
            // 使用collect方法收集将要发送的数据
            ctx.collect(new Event(
                    // 随机取出一个用户名
                    userArray[random.nextInt(userArray.length)],
                    // 随机取出一个url
                    urlArray[random.nextInt(urlArray.length)],
                    // 毫秒时间戳
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 每隔1秒钟发送一条数据
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

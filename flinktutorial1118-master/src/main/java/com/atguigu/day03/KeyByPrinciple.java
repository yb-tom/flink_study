package com.atguigu.day03;

import org.apache.flink.util.MathUtils;

// keyBy的底层原理
public class KeyByPrinciple {
    public static void main(String[] args) {
        // 计算key为0的数据要去的reduce的并行子任务的索引值
        Integer i = new Integer(0);
        int hashCode = i.hashCode();
        System.out.println("hashCode: " + hashCode);
        int murmurHash = MathUtils.murmurHash(hashCode);
        System.out.println("murmurHash: " + murmurHash);
        // 128是默认的最大并行度，4是reduce算子的并行度
        int idx = (murmurHash % 128) * 4 / 128;
        System.out.println("idx: " + idx);
    }
}

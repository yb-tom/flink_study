```
$ mkdir clickhouse
$ cd clickhouse
$ curl -O 'https://builds.clickhouse.com/master/amd64/clickhouse' && chmod a+x ./clickhouse
$ ./clickhouse server
```

再启动一个终端窗口，运行
```
$ ./clickhouse client
```

merge tree

1. 在内存中将数据块排序
2. 追加写入硬盘
3. optimize: 按照归并排序合并数据块

replacing merge tree

1. 在内存中将数据块排序并<span style="color:red">去重</span>
2. 追加写入硬盘
3. optimize: 归并排序+去重

summing merge tree

1. 在内存中将数据块排序并<span style="color:red">聚合</span>
2. 追加写入硬盘
3. optimize: 归并排序+聚合

分布式一致性：

- paxos算法
  - zookeeper中的zab协议
  - raft算法（k8s的etcd，kafka 2.8之后的版本）

《数据密集型应用系统设计》（ddia）

### lab1——mapreduce服务
查看mr文件夹中的文件：
1. master_worker.go: 实现了master节点
2. worker_interface.go: 定义了worker的接口，以及抽象父类
3. map_worker.go、reduce_worker.go: 定义了map类型worker和reduce类型worker
4. 另外 util/ 文件夹中实现了一些工具类: 同步链表SyncList和可重入锁ReentrantRWLock

设计图：

![交互类图.png](asserts%2Flab1%2F%E4%BA%A4%E4%BA%92%E7%B1%BB%E5%9B%BE.png)

![Worker的类图设计和状态图.png](asserts%2Flab1%2FWorker%E7%9A%84%E7%B1%BB%E5%9B%BE%E8%AE%BE%E8%AE%A1%E5%92%8C%E7%8A%B6%E6%80%81%E5%9B%BE.png)
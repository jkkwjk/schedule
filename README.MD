## 分布式任务调度
> 这是一个基于zookeeper的任务发布接收系统

待完善:
- 发布方暂不支持主备切换(只允许一个发布者存在)
- 发布方重启后processed信息会丢失
- 任务完成后的等待机制(现在为强制不分配节点的上一个任务)
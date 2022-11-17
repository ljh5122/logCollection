# logCollection
###olang 实现的一个日志收集服务，由logAgent、logTransfer 两个任务模块组成
**logAgent**
根据本地config 目录下的配置文件，从etcd中获取日志收集任务信息，一个日志收集任务信息会启动一个logAgent任务，logAgent任务从日志文件中读取内容并发送到kafka消息队列

**logTransfer** 根据本地config目录下的配置文件，从etcd中获取日志收集任务信息，一个日志收集任务信息会启动一个logAgent任务，logTransfer任务从kafka队列读取日志内容并写入elasticsearch

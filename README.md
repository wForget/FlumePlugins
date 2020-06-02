## FlumePlugins

flume 扩展插件

### flume-spooldir-source-plugin

+ 实现 HdfsEditsDeserializer，解析 hdfs edit 文件，可以实时监听 hdfs edits 文件目录，收集 hdfs 操作日志。

### flume-smartdata-sink

+ 将 OP_TIMES 操作上报 FileAccessEvent 至 smartdata server。

### flume-high-memory-channel

+ HighMemoryChannel 扩充 MemoryChannel，停止时将 channel 缓冲队列的数据写入文件，启动时读取文件写入缓冲队列，减少重启 Flume 导致的数据丢失。

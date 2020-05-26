## flume-spooldir-source-plugin

该模块通过实现了 spooldir source 的 EventDeserializer，处理特殊文件。

### HdfsEditsDeserializer

实时读取 namenode edits 文件夹，并读取 edits 文件，将 FSEditLogOp 解析为 json 格式字符串。
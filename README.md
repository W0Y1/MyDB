# MyDB
写的是一个时序型数据库，整体架构是仿照InfluxDB写的。
架构大致和下图相符，但是省去了wal部分和retention policy部分，也就是database下的直接是shard。
![image](https://user-images.githubusercontent.com/80105705/230056340-8bbbd873-24b4-4897-b59e-c4fd3bf6196a.png)


不足：1.没有后台线程将位于同一level数据文件和索引文件合并。
2.缺少数据的压缩。
3.有些烂尾的项目，之前写的时候基础知识不够扎实，存在较多内存泄漏问题，运行时容易导致触发断点、崩溃。

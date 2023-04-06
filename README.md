# MyDB
写的是一个时序型数据库，整体架构是仿照InfluxDB写的。
架构大致和下图相符，但是省去了wal部分和retention policy部分，也就是database下的直接是shard。

不足：1.没有后台线程将位于同一level数据文件和索引文件合并。
2.缺少数据的压缩。
3.有些烂尾的项目，之前写的时候基础知识不够扎实，存在较多内存泄漏问题，运行时容易导致触发断点、崩溃。

![image](https://user-images.githubusercontent.com/80105705/230056340-8bbbd873-24b4-4897-b59e-c4fd3bf6196a.png)


LSM树思想：
索引部分 MemTable:倒排索引 SSTable:Tsi文件
数据部分 MemTable:哈希环+map SSTable：Tsm文件

对于新写入的SeriesKey，利用BufferPool中的unordered_map判断和Tsi文件中的布隆过滤器和B+树进行判断是否之前写入过
对于新写入的数据，将其先写入内存中的哈希环

元数据部分：SeriesFile和ShardInfo等
会为每个SeriesKey分配一个SeriesId,方便索引文件。当写入一个新的SeiesKey，会对其做哈希处理，取余数，余数就为所在的partition。而后根据该partition的seq将其作为SeriesId。再根据被分配到的partition下的segement的id和offset得到SeriesOffset(id<<32|offset)



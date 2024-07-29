```txt
1、使用Apache Flink进行数据聚合与分析，并存储到MongoDB
背景:
你手上有10,000条从设备获取的基础数据。这些数据需要通过Apache Flink进行处理和聚合，并最终存储到MongoDB中。你需要设计一个流式数据处理系统，使用Flink对这些数据进行实时处理，生成每5分钟一个点的曲线数据，并将结果存储到MongoDB。

任务:
设计并实现一个使用Apache Flink的数据处理程序，对设备数据进行实时处理和聚合，生成每5分钟一个点的图表数据。并将聚合结果存储到MongoDB中，供后续绘制图表使用。

具体要求:
数据输入: 模拟从Kafka中读取数据，数据格式为JSON，每条数据包括设备ID、时间戳和电量值。
数据处理: 使用Flink对数据进行实时处理和聚合，针对元数据中的每个设备信息字段生成每5分钟一个点的曲线数据。
结果输出: 将聚合结果输出到MongoDB。
代码实现: 提供完整的Flink作业代码
```
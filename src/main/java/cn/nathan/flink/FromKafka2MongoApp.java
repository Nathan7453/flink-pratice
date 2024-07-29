package cn.nathan.flink;

import cn.nathan.flink.func.Kfk2MongoProcessFunc;
import cn.nathan.flink.func.KfkDataItemMapFunc;
import cn.nathan.flink.func.MongoSinkFunc;
import cn.nathan.flink.model.KfkDataItem;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FromKafka2MongoApp {

    private static final Logger log = LoggerFactory.getLogger(FromKafka2MongoApp.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        String strategy = "KAFKA_DS";
        String strategy = "DATA_GEN_DS";

        env.fromSource(DsFactory.ds(strategy), WatermarkStrategy.noWatermarks(), strategy)
            .map(new KfkDataItemMapFunc())
            .assignTimestampsAndWatermarks(KfkDataItemWmStrategy())
            .keyBy(KfkDataItem::getId)
            .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(5)))
            .process(new Kfk2MongoProcessFunc())
            .addSink(new MongoSinkFunc());

        env.execute();
    }

    // 定义 watermark 策略
    private static WatermarkStrategy<KfkDataItem> KfkDataItemWmStrategy() {
        return WatermarkStrategy
            .<KfkDataItem>forBoundedOutOfOrderness(Duration.ofMinutes(1)) // 乱序，等待5s时间
            // 指定时间分配器，要求是 毫秒
            .withTimestampAssigner(
                (SerializableTimestampAssigner<KfkDataItem>) (kfkDataItem, l) -> {
                    if (null != kfkDataItem.getTs()) {
                        return kfkDataItem.getTs() * 1000;
                    } else {
                        return System.currentTimeMillis();
                    }
                });
    }
}

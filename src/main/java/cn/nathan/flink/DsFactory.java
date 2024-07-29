package cn.nathan.flink;

import cn.nathan.flink.model.KfkDataItem;
import cn.nathan.flink.utils.JsonUtils;
import java.math.BigDecimal;
import java.util.Random;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class DsFactory {

    private static final Random ELEC_GEN = new Random(6);

    public static Source<String, ?, ?> ds(String strategy) {
        Source<String, ?, ?> result = null;
        switch (strategy) {
            case "KAFKA_DS":
                result = KafkaSource.<String>builder()
                    .setBootstrapServers("localhost:9092")
                    .setGroupId("gid_elec")
                    .setTopics("topic_elec")
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setStartingOffsets(OffsetsInitializer.earliest()) // 默认就是 earliest
                    .build();
                break;
            case "DATA_GEN_DS":
                result = new DataGeneratorSource<>(
                    (GeneratorFunction<Long, String>) value ->
                        JsonUtils.toJsonString(new KfkDataItem()
                            .setId("TEST_DEV_ID")
                            .setTs(System.currentTimeMillis() / 1000)
                            .setElec(new BigDecimal(ELEC_GEN.nextInt(20)))),
                    Long.MAX_VALUE,
                    RateLimiterStrategy.perSecond(1),
                    Types.STRING);
                break;
        }

        return result;
    }

}

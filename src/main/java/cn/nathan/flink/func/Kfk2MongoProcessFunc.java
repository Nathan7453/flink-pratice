package cn.nathan.flink.func;

import cn.nathan.flink.model.KfkDataItem;
import cn.nathan.flink.model.MongoDataItem;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Kfk2MongoProcessFunc extends
    ProcessWindowFunction<KfkDataItem, MongoDataItem, String, TimeWindow> {

    private static final Logger log = LoggerFactory.getLogger(Kfk2MongoProcessFunc.class);

    @Override
    public void process(String devId,
        ProcessWindowFunction<KfkDataItem, MongoDataItem, String, TimeWindow>.Context context,
        Iterable<KfkDataItem> elements, Collector<MongoDataItem> out) throws Exception {
        List<KfkDataItem> dataItems = StreamSupport.stream(
                elements.spliterator(), false)
            .collect(Collectors.toList());
        log.info(">>> 窗口数据处理: {}, {}", devId, dataItems.size());

        MongoDataItem result = new MongoDataItem()
            .setDevId(devId)
            .setStart(context.window().getStart() / 1000)
            .setEnd(context.window().getEnd() / 1000)
            .setItems(dataItems);

        BigDecimal max = dataItems.stream()
            .map(KfkDataItem::getElec)
            .filter(Objects::nonNull)
            .max(BigDecimal::compareTo)
            .orElseGet(() -> new BigDecimal("-1"));
        result.setMaxV(max);

        out.collect(result);
    }
}

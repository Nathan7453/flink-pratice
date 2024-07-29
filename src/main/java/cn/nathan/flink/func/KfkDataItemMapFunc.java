package cn.nathan.flink.func;

import cn.nathan.flink.model.KfkDataItem;
import cn.nathan.flink.utils.JsonUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class KfkDataItemMapFunc implements MapFunction<String, KfkDataItem> {

    @Override
    public KfkDataItem map(String s) throws Exception {
        KfkDataItem item = null;
        try {
            item = JsonUtils.fromJson(s, KfkDataItem.class);
        } catch (Exception e) {
            item = new KfkDataItem();
        }
        return item;
    }
}

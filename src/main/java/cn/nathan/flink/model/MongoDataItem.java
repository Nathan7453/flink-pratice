package cn.nathan.flink.model;

import cn.nathan.flink.utils.JsonUtils;
import java.math.BigDecimal;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class MongoDataItem {

    // 设备ID
    private String devId;

    // 开始时间戳
    private Long start;

    // 结束时间戳
    private Long end;

    // 最大值电量
    private BigDecimal maxV;

    // 源数据
    private List<KfkDataItem> items;

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}

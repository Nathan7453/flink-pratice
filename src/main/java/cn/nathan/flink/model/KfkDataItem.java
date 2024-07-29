package cn.nathan.flink.model;

import cn.nathan.flink.utils.JsonUtils;
import java.math.BigDecimal;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class KfkDataItem {

    // 设备ID
    private String id;

    // 时间戳
    private Long ts;

    // 电量
    private BigDecimal elec;

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }

    public static void main(String[] args) {
        KfkDataItem dataItem = new KfkDataItem();
        dataItem.setId("kkkk");
        dataItem.setElec(BigDecimal.TEN);
        dataItem.setTs(System.currentTimeMillis() / 1000);
        System.out.println(dataItem);
    }
}

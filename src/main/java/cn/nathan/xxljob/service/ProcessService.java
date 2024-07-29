package cn.nathan.xxljob.service;

import cn.nathan.xxljob.model.ProcessData;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessService {

    private static final Logger log = LoggerFactory.getLogger(ProcessService.class);

    public List<ProcessData> getProcessList(int shardIndex, int shardTotal, int count) {
        // select * from xx where id % #{shareTotal} = #{shareIndex} and status = 0 limit #{count}
        return new ArrayList<>();
    }

    public boolean startTask(String id) {
        // 通过mysql任务记录ID和状态实现乐观锁进而实现幂等
        // update xx set status = 1 where status in (0) and id = #{id}
        return false;
    }

    public void processFinish(String id) {
        // update xx set status = 2 where status in (0, 1) and id = #{id}
        // 释放锁
    }

    public void handleTask(ProcessData process) {
        log.info("业务逻辑处理: id = {}", process.getId());
    }
}

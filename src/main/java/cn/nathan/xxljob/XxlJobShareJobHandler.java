package cn.nathan.xxljob;

import cn.nathan.xxljob.model.ProcessData;
import cn.nathan.xxljob.service.ProcessService;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XxlJobShareJobHandler {

    private static final Logger log = LoggerFactory.getLogger(XxlJobShareJobHandler.class);

    private static final ProcessService processService = new ProcessService();
    private static ThreadPoolExecutor executor = null;

    static {
        executor = new ThreadPoolExecutor(
            4, 8, 0L, TimeUnit.MINUTES,
            new LinkedBlockingDeque<>(), new DiscardPolicy() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                log.warn(">> 队列已满，丢弃任务 <<");
                super.rejectedExecution(r, e);
            }
        });
    }

    @XxlJob("shareJobHandler")
    public void shareJobHandler() throws InterruptedException {
        String param = XxlJobHelper.getJobParam();
        if (StringUtils.isBlank(param)) {
            XxlJobHelper.log("任务参数为空");
            XxlJobHelper.handleFail();
            return;
        }

        // 执行任务节点数量
        int count = Integer.parseInt(param);

        // 分片序号（当前执行器序号）
        int shardIndex = XxlJobHelper.getShardIndex();

        // 分片总数（执行器总数）
        int shardTotal = XxlJobHelper.getShardTotal();

        // 1. 分片获取当前执行器需要执行的所有任务
        List<ProcessData> processList = processService.getProcessList(
            shardIndex, shardTotal, count);

        // 通过JUC工具类阻塞直到所有任务执行完
        CountDownLatch countDownLatch = new CountDownLatch(processList.size());

        // 遍历所有任务
        processList.forEach(process -> {
            String id = process.getId();

            // 以多线程的方式执行所有任务
            executor.execute(() -> {
                try {
                    // 2. 尝试抢占任务（通过乐观锁实现）
                    boolean res = processService.startTask(id);
                    if (!res) {
                        XxlJobHelper.log("任务抢占失败: {}", id);
                        return;
                    }

                    // processing 业务处理逻辑
                    processService.handleTask(process);

                    // 6. 更新任务状态
                    processService.processFinish(id);
                } finally {
                    countDownLatch.countDown();
                }
            });
        });

        // 阻塞直到所有方法执行完成
        countDownLatch.await(5, TimeUnit.MINUTES);
    }
}

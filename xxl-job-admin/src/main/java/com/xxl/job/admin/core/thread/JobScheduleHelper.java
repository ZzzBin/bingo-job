package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.scheduler.ScheduleTypeEnum;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.dao.XxlJobInfoDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper {
    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    private static JobScheduleHelper instance = new JobScheduleHelper();

    public static JobScheduleHelper getInstance() {
        return instance;
    }

    public static final long PRE_READ_MS = 5000;    // pre read

    private Thread scheduleThread;
    private volatile boolean toStop = false;

    /**
     * 判断任务是否到了执行调度的时间
     */
    public void start() {
        Thread scheduleThread = new Thread(this::schedule, "scheduler");
        scheduleThread.setDaemon(true);
        scheduleThread.start();
    }

    public void schedule() {
        while (!toStop) {
            boolean interrupted = Thread.currentThread().isInterrupted();
            if (interrupted) {
                logger.warn("schedule has been interrupted");
                break;
            }
            try {
                // 最低跨度为秒，所以每整秒执行一次
                TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                XxlJobAdminConfig adminConfig = XxlJobAdminConfig.getAdminConfig();
                List<XxlJobGroup> allGroup = adminConfig.getXxlJobGroupDao().findAll();
                for (XxlJobGroup group : allGroup) {
                    if (!StringUtils.hasText(group.getAddressList())) {
                        continue;
                    }
                    // 获取group下所有的job，判断是否要执行
                    XxlJobInfoDao xxlJobInfoDao = adminConfig.getXxlJobInfoDao();
                    List<XxlJobInfo> jobs = xxlJobInfoDao.getJobsByGroup(group.getId());
                    for (XxlJobInfo job : jobs) {
                        if (job.getTriggerStatus() == 1 && judge(job)) {
                            // TODO：高级配置待实现
                            JobTriggerPoolHelper.trigger(job.getId(), TriggerTypeEnum.CRON, job.getExecutorFailRetryCount(), null, job.getExecutorParam(), group.getAddressList());
                            xxlJobInfoDao.update(job);
                        }
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("schedule has been interrupted");
                toStop = true;
            } catch (Exception e) {
                logger.error("schedule exception", e);
            }
        }
    }

    /**
     * 简易实现，后续优化
     */
    private boolean judge(XxlJobInfo job) {
        switch (ScheduleTypeEnum.match(job.getScheduleType(), ScheduleTypeEnum.NONE)) {
            // 固定速度
            case FIX_RATE:
                long triggerLastTime = job.getTriggerLastTime();
                Integer fixTime = Integer.valueOf(job.getScheduleConf()) * 1000;
                // 已经超过了fixTime，立即触发
                long now = System.currentTimeMillis();
                if ((now - triggerLastTime) >= fixTime) {
                    job.setTriggerLastTime(now);
                    return true;
                }
                // 没超过情况下
                return false;
        }
        return false;
    }

    public void toStop() {

    }

    // ---------------------- tools ----------------------
    public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
            Date nextValidTime = new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
            return nextValidTime;
        } else if (ScheduleTypeEnum.FIX_RATE == scheduleTypeEnum /*|| ScheduleTypeEnum.FIX_DELAY == scheduleTypeEnum*/) {
            return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf())*1000 );
        }
        return null;
    }
}

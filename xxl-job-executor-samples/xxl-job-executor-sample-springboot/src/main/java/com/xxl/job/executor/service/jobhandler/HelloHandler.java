package com.xxl.job.executor.service.jobhandler;

import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @Author zhangbin
 * @Date 2022-04-09 15:00
 * @Description:
 */
@Component

public class HelloHandler {

    private static final Logger logger = LoggerFactory.getLogger(XxlJobSpringExecutor.class);

    @XxlJob("helloJobHandler")
    public void helloJobHandler() {
        logger.info("hello " + XxlJobHelper.getJobParam());
    }

    @XxlJob("childHandler")
    public void childHandler() {
        logger.info("childHandler hello");
    }

    @XxlJob("child2Handler")
    public void child2Handler() {
        logger.info("child2222Handler hello");
    }
}

package com.didi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by didi on 2017/7/10.
 */

public class TestLog4jToKafka {
    private static Logger logger = LoggerFactory.getLogger(TestLog4jToKafka.class);

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i <= 10; i++) {
            logger.info("This is Message from process {}.", Thread.currentThread().getName());
            Thread.sleep(1000);
        }

        // kafka log

        logger.info("[KAFKA LOG][SPARK_YARN_APP_IDEnv]={}", System.getenv("spark.yarn.app.id1"));
        logger.info("[KAFKA LOG][SPARK_YARN_APP_IDProperty]={}", System.getProperty("spark.yarn.app.id2"));

        logger.info("[KAFKA LOG][NM_HOSTEnv]={}", System.getenv("NM_HOST1"));
        logger.info("[KAFKA LOG][NM_HOSTProperty]={}", System.getProperty("NM_HOST2"));

        logger.info("[KAFKA LOG][spark.master]={}", System.getProperty("spark.master"));
        logger.info("[KAFKA LOG][spark.yarn.app.id]={}", System.getProperty("spark.yarn.app.id"));
        logger.info("[KAFKA LOG][spark.submit.deployMode]={}", System.getProperty("spark.submit.deployMode"));
    }
}

package com.rann;

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
        logger.info("[KAFKA LOG][NM_HOST]={}", System.getenv("NM_HOST"));
        logger.info("[KAFKA LOG][spark.master]={}", System.getProperty("spark.master"));
        logger.info("[KAFKA LOG][spark.yarn.app.id]={}", System.getProperty("spark.yarn.app.id"));
        logger.info("[KAFKA LOG][spark.submit.deployMode]={}", System.getProperty("spark.submit.deployMode"));
    }
}

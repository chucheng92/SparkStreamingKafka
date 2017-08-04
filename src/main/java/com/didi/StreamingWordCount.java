package com.didi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Hello Spark.
 */
public class StreamingWordCount {
    private static Logger logger = LoggerFactory.getLogger(StreamingWordCount.class);

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("StreamingWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        ssc.checkpoint(".");

        // initial state rdd
        List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<String, Integer>("hello", 100), new Tuple2<String, Integer>("word", 100));
        JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1));

        // update function
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> updateFunc = (word, count, state) -> {
            int sum = count.orElse(0) + (state.exists() ? state.get() : 0);
            Tuple2<String, Integer> output = new Tuple2<>(word, sum);
            state.update(sum);
            return output;
        };
        //update in every batch
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
                pairs.mapWithState(StateSpec.function(updateFunc).initialState(initialRDD));
        //print
        stateDstream.print();
        //start
        ssc.start();

        // kafka log
        Map<String, String> env = System.getenv();
        String logUrlStdErr = env.get("SPARK_LOG_URL_STDERR");

        System.out.println("logUrlStdErr=" + logUrlStdErr);
        System.out.println("SPARK_MASTER=" + System.getProperty("spark.master"));

        logger.info("[KAFKA LOG][logUrlStdErr1]={}", logUrlStdErr);
        logger.info("[KAFKA LOG][logUrlStdErr2]={}", System.getProperty("SPARK_LOG_URL_STDERR"));
        logger.info("[KAFKA LOG][SPARK_MASTER]={}", System.getProperty("spark.master"));

        ssc.awaitTermination();
    }
}

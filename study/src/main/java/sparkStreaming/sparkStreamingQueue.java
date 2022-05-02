package sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Queue;

public class sparkStreamingQueue {
    public static void main(String[] args) {
        /**
         * 1.创建环境对象
         * 需要两个参数：环境配置、批量处理的周期（采集周期）
         *
         */
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark streaming");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(3000));
        //StreamingContext streamingContext = new StreamingContext(conf, new Duration(3));


        /**
         * 2.逻辑处理
         */

        //javaStreamingContext.queueStream(new Queue<JavaRDD<Integer>>() {
        //},false)
        //
        ///**
        // * 3.等待采集器结束
        // */
        //javaStreamingContext.start();
        //javaStreamingContext.awaitTermination();
    }
}

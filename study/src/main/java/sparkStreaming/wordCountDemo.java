package sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterable;

import java.util.Arrays;

public class wordCountDemo {
    public static void main(String[] args) throws InterruptedException {
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
        //获取端口数据
        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("localhost", 9999);
        //ReceiverInputDStream<String> receiverInputDStream = streamingContext.
        //        socketTextStream("localhost", 9999, StorageLevel.DISK_ONLY());
        JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>) line -> {
            return Arrays.stream(line.split(" ")).iterator();
        });
        JavaPairDStream<String, Integer> wordToOne = words.mapToPair((PairFunction<String, String, Integer>) word -> {
            return new Tuple2<>(word, 1);
        });
        JavaPairDStream<String, Integer> wordToCount = wordToOne.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);
        wordToCount.print();
        /**
         * 3.等待采集器结束
         */
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}

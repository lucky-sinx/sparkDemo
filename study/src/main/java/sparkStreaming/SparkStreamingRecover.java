package sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingRecover {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark streaming");
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("cp", () -> {
            JavaStreamingContext newJsc = new JavaStreamingContext(conf, new Duration(10000));

            JavaReceiverInputDStream<String> datas = newJsc.socketTextStream("localhost", 9999);
            JavaPairDStream<String, Integer> wto1 = datas.flatMap(line -> Arrays.stream(line.split(" ")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1));

            JavaPairDStream<String, Integer> wc = wto1.reduceByKey((x, y) -> x + y);
            wc.print();
            return newJsc;
        });
        jsc.checkpoint("cp");

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

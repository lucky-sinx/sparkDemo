package sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkStreamingWindow {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark streaming");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(3000));

        JavaReceiverInputDStream<String> datas = jsc.socketTextStream("localhost", 9999);
        JavaPairDStream<String, Integer> wto1 = datas.flatMap(line -> Arrays.stream(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1));

        //设置采样周期，windows大小应为周期的整数倍
        //JavaPairDStream<String, Integer> wto2 = wto1.window(new Duration(3000 * 2));
        //JavaPairDStream<String, Integer> wc = wto2.reduceByKey((x, y) -> x + y);
        //wc.print();

        //通过设置滑动的步长可以避免重复数据的计算
        JavaPairDStream<String, Integer> wto3 = wto1.window(new Duration(3000 * 2),new Duration(3000 * 2));
        JavaPairDStream<String, Integer> wc3 = wto3.reduceByKey((x, y) -> x + y);
        wc3.print();

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

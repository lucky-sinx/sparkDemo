package sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

public class sparkStreamingDIY {
    public static void main(String[] args) {
        /**
         * 1.创建环境对象
         * 需要两个参数：环境配置、批量处理的周期（采集周期）
         *
         */
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark streaming");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, new Duration(3000));

        /**
         * 2
         */
        JavaReceiverInputDStream<String> msgDS = javaStreamingContext.receiverStream(new MyReceiver(StorageLevel.MEMORY_ONLY()));
        msgDS.print();

        /**
         * 3.等待采集器结束
         */
        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class MyReceiver extends Receiver<String> {
    private boolean flag=true;

    public MyReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (flag) {
                    int val = new Random().nextInt();
                    String msg = "采集的数据为：" + val;
                    store(msg);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    @Override
    public void onStop() {
        flag=false;
    }
}


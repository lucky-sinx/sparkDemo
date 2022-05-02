package sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkStreamingClose {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("spark streaming");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(3000));

        //存缓冲区的路径
        jsc.checkpoint("cp");

        JavaReceiverInputDStream<String> datas = jsc.socketTextStream("localhost", 9999);

        //无状态
        //JavaPairDStream<String, Integer> wc = datas.flatMap(line -> Arrays.stream(line.split(" ")).iterator())
        //        .mapToPair(word -> new Tuple2<>(word, 1))
        //        .reduceByKey((x, y) -> x + y);

        //有状态,map操作可以照常进行，但是reduce需要updateStata
        JavaPairDStream<String, Integer> wto1 = datas.flatMap(line -> Arrays.stream(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> wc = wto1.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> integers, Optional<Integer> integerOptional) throws Exception {
                int res = integerOptional.orNull() == null ? 0 : integerOptional.get();
                for (int num : integers) res += num;
                return Optional.of(res);
            }
        });

        wc.print();

        jsc.start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                //while (true){
                //    if(true){//从第三方来判断是否需要关闭的条件
                //        StreamingContextState state = jsc.getState();
                //        if(state==StreamingContextState.ACTIVE){
                //            jsc.stop(true,true);
                //            System.exit(0);
                //        }
                //    }
                //}
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                StreamingContextState state = jsc.getState();
                if(state==StreamingContextState.ACTIVE){
                    jsc.stop(true,true);
                }
                jsc.stop(true,true);
                System.exit(0);
            }
        }).start();

        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

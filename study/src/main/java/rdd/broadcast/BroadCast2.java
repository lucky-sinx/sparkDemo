package rdd.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 使用广播变量
 */
public class BroadCast2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> pairRDD1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("c", 3),
                new Tuple2<>("f", 4)
        ));
        JavaPairRDD<String, Integer> pairRDD2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 4),
                new Tuple2<>("b", 5),
                new Tuple2<>("c", 6),
                new Tuple2<>("f", 7)
        ));
        //使用join
        pairRDD1.join(pairRDD2).collect().forEach(System.out::println);
        //把RDD2作为本地map来对RRD1进行map操作
        Map<String,Integer> map=new HashMap<>();
        pairRDD2.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                map.put(stringIntegerTuple2._1(), stringIntegerTuple2._2());
            }
        });
        Broadcast<Map<String, Integer>> broadcastMap = sc.broadcast(map);

        pairRDD1.map(new Function<Tuple2<String, Integer>, Tuple2<String,int[]>>() {
            @Override
            public Tuple2<String,int[]> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                String key = stringIntegerTuple2._1();
                Integer value = stringIntegerTuple2._2();
                Tuple2<String, int[]> res = new Tuple2<String, int[]>(key,new int[]{value,broadcastMap.value().getOrDefault(key,-1)});
                return res;
            }
        }).collect().forEach(new Consumer<Tuple2<String, int[]>>() {
            @Override
            public void accept(Tuple2<String, int[]> stringTuple2) {
                System.out.printf("%s:%d,%d\n",stringTuple2._1(),stringTuple2._2()[0],stringTuple2._2()[1]);
            }
        });
    }
}

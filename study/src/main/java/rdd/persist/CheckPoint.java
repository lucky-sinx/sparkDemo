package rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class CheckPoint {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setCheckpointDir("checkpoint1.txt");
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello hi", "hello spark"));

        JavaRDD<String> flatRDD = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> mapRDD = flatRDD.mapToPair((PairFunction<String, String, Integer>) s -> {
            System.out.println("@@@@@@@@@@@");
            return new Tuple2<>(s, 1);
        });
        //mapRDD.rdd.persist(StorageLevel.DISK_ONLY());//临时文件作业执行完后删除
        //mapRDD.cache();//持久化操作，重复使用mapRDD
        mapRDD.checkpoint();//指定文件，作业完成后不会删除

        System.out.println(mapRDD.toDebugString());

        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> (x + y));
        reduceRDD.foreach((VoidFunction<Tuple2<String, Integer>>) t->System.out.printf("Word:%s,cnt:%d\n",t._1(),t._2()));
        System.out.println(mapRDD.toDebugString());

        System.out.println("***************");
        JavaPairRDD<String, Iterable<Integer>> groupRDD1 = mapRDD.groupByKey();
        groupRDD1.collect().forEach(System.out::println);
        sc.stop();
    }
}

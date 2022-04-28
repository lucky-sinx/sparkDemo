package rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

public class Persist1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello hi", "hello spark"));

        JavaRDD<String> flatRDD = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> mapRDD = flatRDD.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> (x + y));
        reduceRDD.foreach((VoidFunction<Tuple2<String, Integer>>) t->System.out.printf("Word:%s,cnt:%d\n",t._1(),t._2()));

        System.out.println("***************");

        JavaRDD<String> flatRDD1 = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> mapRDD1 = flatRDD1.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        //JavaPairRDD<String, Integer> reduceRDD1 = mapRDD1.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> (x + y));
        JavaPairRDD<String, Iterable<Integer>> groupRDD1 = mapRDD1.groupByKey();
        groupRDD1.collect().forEach(System.out::println);
    }
}

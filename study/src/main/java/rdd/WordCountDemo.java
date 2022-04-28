package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取本地的一个文件
        JavaRDD<String> lines = sc.textFile("D:\\code\\java\\sparkDemo\\study\\src\\main\\resources\\wordcount");
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordCountOne = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        wordCountOne.collect().forEach(System.out::println);
        JavaPairRDD<String, Integer> wordCounts = wordCountOne.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> (x + y));
        wordCounts.collect().forEach(System.out::println);
        //wordCounts.saveAsTextFile("D:\\code\\java\\sparkDemo\\study\\src\\main\\resources\\wcOutput");
    }
}

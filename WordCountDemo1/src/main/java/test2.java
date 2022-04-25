import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class test2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("sparkBoot1").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("D:\\code\\java\\sparkDemo\\WordCountDemo1\\src\\main\\resources\\1.txt").cache();

        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(Pattern.compile(" ").split(s)).iterator());
        JavaPairRDD<String, Integer> wordCntOne = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> wordCounts = wordCntOne.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        wordCounts.saveAsTextFile("D:\\code\\java\\sparkDemo\\WordCountDemo1\\src\\main\\resources\\2_wordCount.txt");
    }
}

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class RddValueDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        JavaRDD<String> rdd3 = sc.parallelize(Arrays.asList("aaa bbb ccc", "ccc ddd aaa"));
        JavaRDD<Person> rdd4 = sc.parallelize(Arrays.asList(
                new Person("name1", 1),
                new Person("name2", 2),
                new Person("name3", 3),
                new Person("name4", 4),
                new Person("name5", 5)
        ));
        JavaRDD<String> rdd5 = sc.parallelize(Arrays.asList("hello hi", "hello spark"));


        JavaPairRDD<String, Integer> pairs0 = sc.parallelizePairs(Arrays.asList(
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
        JavaPairRDD<String, Integer> pairs1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("panda", 0),
                new Tuple2<String, Integer>("pink", 3),
                new Tuple2<String, Integer>("pirate", 3),
                new Tuple2<String, Integer>("panda", 1),
                new Tuple2<String, Integer>("pink", 2)));
        JavaPairRDD<String, Integer> pairs2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("panda", 0),
                new Tuple2<String, Integer>("pink", 3),
                new Tuple2<String, Integer>("pirate", 3),
                new Tuple2<String, Integer>("panda", 1),
                new Tuple2<String, Integer>("pink", 2)));
        JavaPairRDD<String, String> pairs3 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("panda", "a"),
                new Tuple2<String, String>("pink", "b"),
                new Tuple2<String, String>("panda", "c"),
                new Tuple2<String, String>("pink", "d"),
                new Tuple2<String, String>("hello", "e")));
        JavaPairRDD<String, String> pair5 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("aaa", "123"),
                new Tuple2<String, String>("bbb", "123"),
                new Tuple2<String, String>("ccc", "123"),
                new Tuple2<String, String>("eee", "123"),
                new Tuple2<String, String>("dba", "123")
        ), 3);
    }
}

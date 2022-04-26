import org.apache.spark.Accumulable;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.junit.Test;

import java.util.Arrays;

public class AccDemo {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    JavaSparkContext sc = new JavaSparkContext(conf);

    @Test
    public void testAccumulator() {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
            "hello11",
            "",
            "hello12",
            "hello13",
            "hello14",
            "hello15"
        ));
        Accumulator<Integer> accumulator = sc.accumulator(0);
        rdd.flatMap((FlatMapFunction<String, String>) s->{
            if(s.equals(""))accumulator.add(1);
            return Arrays.stream(s.split(" ")).iterator();
        }).collect().forEach(System.out::println);
        rdd.flatMap((FlatMapFunction<String, String>) s->{
            if(s.equals(""))accumulator.add(1);
            return Arrays.stream(s.split(" ")).iterator();
        }).collect().forEach(System.out::println);
        System.out.println("Blank lines:" + accumulator.value());
    }
}

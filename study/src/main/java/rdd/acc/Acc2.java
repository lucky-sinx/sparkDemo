package rdd.acc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class Acc2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        LongAccumulator longAccumulator = sc.sc().longAccumulator();
        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                longAccumulator.add(1L);
            }
        });
        System.out.println(longAccumulator.value());
    }
}

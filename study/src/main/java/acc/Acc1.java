package acc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.function.Consumer;

public class Acc1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        final int[] sum = {0};
        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum[0] = integer + sum[0];
            }
        });
        System.out.println(sum[0]);
        rdd.collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                sum[0] = integer + sum[0];
            }
        });
        System.out.println(sum[0]);
    }
}

package acc;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class Acc3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        Accumulator<Integer> accumulator = sc.accumulator(0);

        //转换算子没有执行算子就不会执行，导致少加
        JavaRDD<Integer> mapRDD = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                accumulator.add(integer);
                return integer;
            }
        });
        System.out.println(accumulator.value());

        //多加减的情况
        mapRDD.collect();
        mapRDD.collect();
        System.out.println(accumulator.value());


    }
}

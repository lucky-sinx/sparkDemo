package acc;

import org.apache.commons.lang3.text.translate.NumericEntityUnescaper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;
import scala.tools.cmd.Spec;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * wordCount
 */
public class Acc4 {
    protected static SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    protected static JavaSparkContext sc = new JavaSparkContext(conf);
    public static void main(String[] args) {

        JavaRDD<String> wordRDD = sc.parallelize(Arrays.asList("hello", "hi", "ok", "hello"));

        /**
         * 传统方法的wordCount
         */
        wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).collect().forEach(System.out::println);
        /**
         * 使用自定义的累加器
         */
        MyAccWordCount myAccWordCount = new MyAccWordCount();
        sc.sc().register(myAccWordCount,"wordCount");//注册

        wordRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                myAccWordCount.add(s);
            }
        });
        System.out.println(myAccWordCount.value());
    }
}
class MyAccWordCount extends AccumulatorV2<String, Map<String,Integer>>{
    Map<String,Integer> map=new HashMap<>();

    @Override
    public boolean isZero() {
        //判断是否是初始状态
        return map.isEmpty();
    }

    @Override
    public AccumulatorV2<String, Map<String, Integer>> copy() {
        MyAccWordCount myAccWordCount = new MyAccWordCount();
        myAccWordCount.map=map;
        return myAccWordCount;
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void add(String v) {
        Integer integer = map.getOrDefault(v, 0)+1;
        map.put(v,integer);
    }

    @Override
    public void merge(AccumulatorV2<String, Map<String, Integer>> other) {
        Map<String, Integer> map1 = this.map;
        Map<String, Integer> map2 = other.value();
        map2.forEach(new BiConsumer<String, Integer>() {
            @Override
            public void accept(String s, Integer integer) {
                map1.put(s,map1.getOrDefault(s, 0)+integer);
            }
        });
    }

    @Override
    public Map<String, Integer> value() {
        return map;
    }
}

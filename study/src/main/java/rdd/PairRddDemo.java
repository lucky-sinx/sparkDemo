package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PairRddDemo {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    JavaSparkContext sc = new JavaSparkContext(conf);

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

    @Test
    public void testCreatePairRdd() {
        //使用第一个单词作为key创建PairRdd
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("aaa bbb vvv", "aaa sss ddd", "bbb aaa"));
        JavaPairRDD<String, String> pairRdd1 = rdd.mapToPair((PairFunction<String, String, String>) s -> new Tuple2<>(s.split(" ")[0], s));
        JavaPairRDD<String, List<String>> pairRDD2 = rdd.mapToPair((PairFunction<String, String, List<String>>) s -> new Tuple2<>(s.split(" ")[0], Arrays.asList(s.split(" "))));
        pairRdd1.collect().forEach(System.out::println);
        pairRDD2.collect().forEach(System.out::println);
    }

    @Test
    public void testFilterTheSecond() {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("xiaobai is a big boss", "is it right", "sure you are right"));
        JavaPairRDD<String, String> sPair = rdd.mapToPair((PairFunction<String, String, String>) s -> new Tuple2<>(s.split(" ")[0], s));
        System.out.println(sPair.collect());

        //对第二个元素进行筛选

        //只有在Main中下面的代码才可以通过,原因尚不了解
        //Function<Tuple2<String,String>,Boolean> lengthFilter=new Function<Tuple2<String, String>, Boolean>() {
        //    @Override
        //    public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
        //        return stringStringTuple2._2().length()<20;
        //    }
        //};

        System.out.println(sPair.filter(new filterLength()).collect());

    }

    @Test
    public void testAvg() {
        List<Tuple2<String, int[]>> collect = pairs1.mapValues((Function<Integer, int[]>) x -> new int[]{x, 1}).collect();
        JavaPairRDD<String, int[]> avgRDD = pairs1.mapValues((Function<Integer, int[]>) x -> new int[]{x, 1}).reduceByKey((Function2<int[], int[], int[]>) (a, b) -> new int[]{a[0] + b[0], a[1] + b[1]});
        List<Tuple2<String, int[]>> collect1 = avgRDD.collect();
        avgRDD.collect().forEach((stringTuple2 -> {
            System.out.println(stringTuple2._1() + " avg: " + stringTuple2._2()[0] / (double) stringTuple2._2()[1]);
        }));
    }

    @Test
    public void testCombineByKey() {
        JavaPairRDD<String, int[]> avgRDD = pairs1.combineByKey(
                (Function<Integer, int[]>) x -> new int[]{x, 1}, //
                (Function2<int[], Integer, int[]>) (a, b) -> new int[]{a[0] + b, a[1] + 1},
                (Function2<int[], int[], int[]>) (a, b) -> new int[]{a[0] + b[0], a[1] + b[1]}
        );
        avgRDD.collect().forEach((stringTuple2 -> {
            System.out.println(stringTuple2._1() + " avg: " + stringTuple2._2()[0] / (double) stringTuple2._2()[1]);
        }));
    }

    @Test
    public void testGroupByKey() {
        pairs1.groupByKey().collect().forEach(System.out::println);
    }

    @Test
    public void testReduceByKey() {
        pairs1.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> (x + y)).collect().forEach(System.out::println);
        //使用groupByKey可以达到同样的效果，但是效率不及reduceByKey
        pairs1.groupByKey().mapValues((Function<Iterable<Integer>, Integer>) i -> {
            int res = 0;
            Iterator<Integer> iterator = i.iterator();
            while (iterator.hasNext()) {
                res += iterator.next();
            }
            return res;
        }).collect().forEach(System.out::println);
    }

    @Test
    public void testCoGroup() {
        //是join操作的关键
        pairs1.cogroup(pairs3).collect().forEach(System.out::println);
    }

    @Test
    public void testJoin() {
        pairs1.join(pairs3).collect().forEach(System.out::println);
        pairs1.leftOuterJoin(pairs3).collect().forEach(System.out::println);
        pairs1.rightOuterJoin(pairs3).collect().forEach(System.out::println);
    }

    @Test
    public void testSortByKey(){
        pairs3.sortByKey().collect().forEach(System.out::println);
    }

    @Test
    public void testMapValues(){
        pairs3.mapValues((Function<String, Integer>) s->1).collect().forEach(System.out::println);
    }

}

class filterLength implements Function<Tuple2<String, String>, Boolean> {

    @Override
    public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
        return stringStringTuple2._2().length() < 20;
    }
}

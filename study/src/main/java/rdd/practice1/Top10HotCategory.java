package rdd.practice1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Consumer;

public class Top10HotCategory {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = sc.textFile("D:\\code\\java\\sparkDemo\\study\\src\\main\\java\\rdd.practice1\\user_visit_action.txt");
        //1.切分原始数据
        JavaRDD<String[]> dataRDD = fileRDD.map((Function<String, String[]>) s -> s.split("_"));

        //2.统计品类的点击数量
        JavaRDD<String[]> actionRDD = dataRDD.filter((Function<String[], Boolean>) strings -> {
            return !strings[6].equals("-1");
        });//为-1说明不是点击数据

        JavaPairRDD<String, Integer> clickCntRDD = actionRDD
                .mapToPair((PairFunction<String[], String, Integer>) strings -> new Tuple2<>(strings[6], 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> (a + b));

        System.out.println(clickCntRDD.take(5));

        //3.统计下单数量
        JavaRDD<String[]> orderRDD = dataRDD.filter((Function<String[], Boolean>) strings -> {
            return !strings[8].equals("null");
        });

        JavaPairRDD<String, Integer> orderCntRDD = orderRDD
                .flatMap((FlatMapFunction<String[], String>) strings -> Arrays.stream(strings[8].split(",")).iterator())  //获取这个订单额所有品类并展开flat
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> (a + b));

        System.out.println(orderCntRDD.take(5));

        //4.统计支付行为
        JavaRDD<String[]> payRDD = dataRDD.filter((Function<String[], Boolean>) strings -> {
            return !strings[10].equals("null");
        });
        JavaPairRDD<String, Integer> payCntRDD = payRDD
                .flatMap((FlatMapFunction<String[], String>) strings -> Arrays.stream(strings[10].split(",")).iterator())  //获取这个订单额所有品类并展开flat
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> (a + b));

        System.out.println(payCntRDD.take(5));
        //5.排序取前5
        JavaPairRDD<String, int[]> coGroupRDD = clickCntRDD.cogroup(orderCntRDD, payCntRDD)
                .mapValues(new Function<Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>, int[]>() {
                    @Override
                    public int[] call(Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>> iterableIterableIterableTuple3) throws Exception {
                        Iterator<Integer> a = iterableIterableIterableTuple3._1().iterator();
                        Iterator<Integer> b = iterableIterableIterableTuple3._2().iterator();
                        Iterator<Integer> c = iterableIterableIterableTuple3._3().iterator();
                        return new int[]{(a.hasNext() ? a.next() : 0), (b.hasNext() ? b.next() : 0), (c.hasNext() ? c.next() : 0)};
                    }
                });
        coGroupRDD.take(10).forEach((Consumer<? super Tuple2<String, int[]>>) tuple -> {
            System.out.printf("%s:%d,%d,%d\n", tuple._1(), tuple._2()[0], tuple._2()[1], tuple._2()[2]);
        });

        JavaPairRDD<int[], String> resultRDD = coGroupRDD.
                mapToPair((PairFunction<Tuple2<String, int[]>, int[], String>) t -> new Tuple2<>(t._2(), t._1())).//交换key-value，使用value排序
                        sortByKey(new MyComparator());

        resultRDD.collect().forEach((Consumer<? super Tuple2<int[], String>>) tuple -> {
            System.out.printf("%s:%d,%d,%d\n", tuple._2(), tuple._1()[0], tuple._1()[1], tuple._1()[2]);
        });
    }
}

class MyComparator implements Comparator<int[]>, Serializable {

    @Override
    public int compare(int[] o1, int[] o2) {
        if (o1[0] != o2[0]) return o2[0] - o1[0];
        else if (o1[1] != o2[1]) return o2[1] - o1[1];
        else return o2[2] - o1[2];
    }
}

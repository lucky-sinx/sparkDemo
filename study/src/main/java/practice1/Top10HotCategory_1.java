package practice1;

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

public class Top10HotCategory_1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = sc.textFile("D:\\code\\java\\sparkDemo\\study\\src\\main\\java\\practice1\\user_visit_action.txt");
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

        //不用coGroup
        JavaPairRDD<String, int[]> expandRDD = clickCntRDD.mapToPair((PairFunction<Tuple2<String, Integer>, String, int[]>) tuple -> {
            return new Tuple2<>(tuple._1(), new int[]{tuple._2(), 0, 0});
        }).union(orderCntRDD.mapToPair((PairFunction<Tuple2<String, Integer>, String, int[]>) tuple -> {
            return new Tuple2<>(tuple._1(), new int[]{0, tuple._2(), 0});
        })).union(payCntRDD.mapToPair((PairFunction<Tuple2<String, Integer>, String, int[]>) tuple -> {
            return new Tuple2<>(tuple._1(), new int[]{0, 0, tuple._2()});
        })).reduceByKey((Function2<int[], int[], int[]>) (a,b)->{
            return new int[]{a[0]+b[0],a[1]+b[1],a[2]+b[2]};
        });

        expandRDD.collect().forEach((Consumer<? super Tuple2<String, int[]>>) tuple -> {
            System.out.printf("%s:[%d,%d,%d]\n", tuple._1(), tuple._2()[0], tuple._2()[1], tuple._2()[2]);
        });

        JavaPairRDD<int[], String> resultRDD = expandRDD.
                mapToPair((PairFunction<Tuple2<String, int[]>, int[], String>) t -> new Tuple2<>(t._2(), t._1())).//交换key-value，使用value排序
                        sortByKey(new MyComparator());

        resultRDD.collect().forEach((Consumer<? super Tuple2<int[], String>>) tuple -> {
            System.out.printf("%s:%d,%d,%d\n", tuple._2(), tuple._1()[0], tuple._1()[1], tuple._1()[2]);
        });
    }
}


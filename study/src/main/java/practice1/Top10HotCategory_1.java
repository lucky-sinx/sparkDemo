package practice1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.function.Consumer;

public class Top10HotCategory_1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = sc.textFile("D:\\code\\java\\sparkDemo\\study\\src\\main\\java\\practice1\\user_visit_action.txt");
        //1.切分原始数据
        JavaRDD<String[]> dataRDD = fileRDD.map((Function<String, String[]>) s -> s.split("_"));

        //2.一次将品类的点击、下单、支付统计成(?,?,?)的形式
        JavaPairRDD<String, int[]> expandRDD = dataRDD.flatMapToPair(new PairFlatMapFunction<String[], String, int[]>() {
            @Override
            public Iterator<Tuple2<String, int[]>> call(String[] strings) throws Exception {
                List<Tuple2<String, int[]>> res = new LinkedList<>();
                if (!strings[6].equals("-1")) {
                    res.add(new Tuple2<>(strings[6], new int[]{1, 0, 0}));
                } else if (!strings[8].equals("null")) {
                    Arrays.stream(strings[8].split(",")).forEach((Consumer<? super String>) s -> {
                        res.add(new Tuple2<>(s, new int[]{0, 1, 0}));
                    });
                } else if (!strings[10].equals("null")) {
                    Arrays.stream(strings[10].split(",")).forEach((Consumer<? super String>) s -> {
                        res.add(new Tuple2<>(s, new int[]{0, 0, 1}));
                    });
                }
                return res.iterator();
            }
        }).reduceByKey(new Function2<int[], int[], int[]>() {
            @Override
            public int[] call(int[] ints, int[] ints2) throws Exception {
                return new int[]{ints[0] + ints2[0], ints[1] + ints2[1], ints[2] + ints2[2]};
            }
        });

        expandRDD.collect().forEach((Consumer<? super Tuple2<String, int[]>>) tuple -> {
            System.out.printf("%s:[%d,%d,%d]\n", tuple._1(), tuple._2()[0], tuple._2()[1], tuple._2()[2]);
        });

        JavaPairRDD<int[], String> resultRDD = expandRDD.
                mapToPair((PairFunction<Tuple2<String, int[]>, int[], String>) t -> new Tuple2<>(t._2(), t._1())).//交换key-value，使用value排序
                        sortByKey(new MyComparator());

        resultRDD.collect().forEach((Consumer<? super Tuple2<int[], String>>) tuple -> {
            System.out.printf("%s:[%d,%d,%d]\n", tuple._2(), tuple._1()[0], tuple._1()[1], tuple._1()[2]);
        });
    }
}


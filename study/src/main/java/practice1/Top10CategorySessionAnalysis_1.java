package practice1;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class Top10CategorySessionAnalysis_1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = sc.textFile("D:\\code\\java\\sparkDemo\\study\\src\\main\\java\\practice1\\user_visit_action.txt");
        JavaRDD<String[]> dataRDD = fileRDD.map((Function<String, String[]>) s -> s.split("_"));
        dataRDD.cache();

        //1.get top 10
        List<Tuple2<int[], String>> top10 = getTop10(dataRDD, 4);
        HashSet<String> top10Ids = new HashSet<>();
        top10.forEach((Consumer<? super Tuple2<int[], String>>) tuple->top10Ids.add(tuple._2));
        System.out.println(top10Ids);

        //2.过滤前10的session并根据session、id进行计数
        JavaPairRDD<String,Tuple2<String,Integer>> reduceRDD = dataRDD.filter((Function<String[], Boolean>) datas -> {
            return !datas[6].equals("-1") && top10Ids.contains(datas[6]);
        }).mapToPair((PairFunction<String[], Tuple2<String,String>, Integer>) datas -> {
            return new Tuple2<>(new Tuple2<>(datas[6],datas[2]), 1);
        }).reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y).mapToPair((PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>) tuple->{
            return new Tuple2<>(tuple._1()._1(),new Tuple2<>(tuple._1()._2(),tuple._2()));
        });
        //3.相同商品类别分组
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = reduceRDD.groupByKey();

        //4.对每一分组进行排序
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> result = groupRDD.mapValues((Function<Iterable<Tuple2<String, Integer>>,Iterable<Tuple2<String, Integer>>>) iter -> {
            List<Tuple2<String, Integer>> list =new ArrayList<>();
            Iterator<Tuple2<String, Integer>> iterator = iter.iterator();
            while (iterator.hasNext()){
                list.add(iterator.next());
            }
            list.sort(new Comparator<Tuple2<String, Integer>>() {
                @Override
                public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                    return o2._2() - o1._2();
                }
            });
            List<Tuple2<String, Integer>> res =new ArrayList<>();
            for(int i=0;i<10&&i<list.size();i++){
                res.add(list.get(i));
            }
            return res;
        });
        result.collect().forEach(System.out::println);
    }

    public static List<Tuple2<int[], String>> getTop10(JavaRDD<String[]> dataRDD, int topN) {
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

        JavaPairRDD<int[], String> resultRDD = expandRDD.
                mapToPair((PairFunction<Tuple2<String, int[]>, int[], String>) t -> new Tuple2<>(t._2(), t._1())).//交换key-value，使用value排序
                        sortByKey(new MyComparator());

        return resultRDD.take(topN);
    }
}

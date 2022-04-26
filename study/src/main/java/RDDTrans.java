import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;

import java.util.*;

public class RDDTrans {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));

    @Test
    public void testMap() {
        //1->1，对每一个元素，map函数返回的是一个元素
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 45));
        rdd.map((Function<Integer, Integer>) x -> x * x).collect().forEach(System.out::println);
    }

    @Test
    public void testFlatMap() {
        //1->多，对每一个输入，flatMap函数返回的值是一个返回值序列的迭代器

        //将数据行切分单词
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("aaa bbb ccc", "ccc ddd aaa"));
        JavaRDD<String> words1 = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator());
        JavaRDD<List<String>> words2 = rdd.map((Function<String, List<String>>) s -> Arrays.asList(s.split(" ")));
        words1.collect().forEach(System.out::println);
        words2.collect().forEach(System.out::println);
    }

    @Test
    public void testReduce() {
        //简单reduce的求和操作
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 45));
        Integer reduce = rdd.reduce((Function2<Integer, Integer, Integer>) (x, y) -> x + y);
        System.out.println(reduce);
    }

    @Test
    public void testFold() {
        //fold的求和操作,每个分区会从zeroValue开始进行类似于reduce的操作,若结果>200,说明是有两个分区
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
        System.out.println(rdd.getNumPartitions());
        Integer fold = rdd.fold(1000, (Function2<Integer, Integer, Integer>) (x, y) -> {
            System.out.printf("??????x=%d,y=%d\n", x, y);
            return (x + y);
        });
        System.out.println(fold);
    }

    @Test
    public void testAggregate() {
        //使用aggregate来计算平均值
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        //使用int[](0,0)来作为初始值
        int[] avg = rdd.aggregate(
                new int[]{0, 0},
                (Function2<int[], Integer, int[]>) (a, x) -> new int[]{a[0] + x, a[1] + 1},//这个是在每个分区本地进行的操作，所以各自算各自的（sum,cnt）
                (Function2<int[], int[], int[]>) (a, b) -> new int[]{a[0] + b[0], a[1] + b[1]}//相当于将所有分区的值进行reduce操作
        );
        System.out.println(avg[0] / (double) avg[1]);
    }

    @Test
    public void testMapPartitions() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).repartition(4);

        System.out.println(rdd.map((Function<Integer, Integer>) integer -> {
            System.out.println(integer);
            return integer * 2;
        }).collect());

        System.out.println(rdd.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) temp -> {
            List<Integer> res = new LinkedList<>();
            temp.forEachRemaining(x -> res.add(x * 2));
            System.out.println(res);
            return res.iterator();
        }).collect());
    }

    @Test
    public void testForEach(){
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),3);
        rdd.repartition(3).foreach((VoidFunction<Integer>) x->{
            System.out.println(x);
        });
    }

}

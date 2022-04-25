import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PairRddDemo {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    JavaSparkContext sc = new JavaSparkContext(conf);
    @Test
    public void testCreatePairRdd(){
        //使用第一个单词作为key创建PairRdd
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("aaa bbb vvv", "aaa sss ddd", "bbb aaa"));
        JavaPairRDD<String, String> pairRdd1 = rdd.mapToPair((PairFunction<String, String, String>) s -> new Tuple2<>(s.split(" ")[0], s));
        JavaPairRDD<String, List<String>> pairRDD2 = rdd.mapToPair((PairFunction<String, String, List<String>>) s -> new Tuple2<>(s.split(" ")[0], Arrays.asList(s.split(" "))));
        pairRdd1.collect().forEach(System.out::println);
        pairRDD2.collect().forEach(System.out::println);
    }

    @Test
    public void testFilterTheSecond(){
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
}

class filterLength implements Function<Tuple2<String,String>,Boolean>{

    @Override
    public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
        return stringStringTuple2._2().length()<20;
    }
}

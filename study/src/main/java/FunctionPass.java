import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FunctionPass {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    JavaSparkContext sc = new JavaSparkContext(conf);

    @Test
    public void test1(){
        //使用匿名内部类进行函数传递
        List<Integer> arrayList = new ArrayList<>(Arrays.asList(1,2,3,4,5,6,7)) ;
        JavaRDD<Integer> rdd1 = sc.parallelize(arrayList);
        //java8的lambda表达式
        JavaRDD<Integer> rdd2 = rdd1.filter((Function<Integer, Boolean>) x -> x >= 3 && x <= 6);
        rdd2.collect().forEach(System.out::println);
    }

    @Test
    public void test2(){
        //使用具名类进行函数传递
        JavaRDD<String> rdd = sc.parallelize(new LinkedList<String>(Arrays.asList("error1", "ok", "error2", "ok", "error4")));
        //JavaRDD<String> errors = rdd.filter((Function<String, Boolean>) s->{return true;});
        JavaRDD<String> errors2 = rdd.filter(new ContainError());
        errors2.collect().forEach(System.out::println);
        //rdd.collect().forEach(System.out::println);
    }

    @Test
    public void test3(){
        //使用带参数的Java函数类
        JavaRDD<String> rdd = sc.parallelize(new LinkedList<String>(Arrays.asList("error1", "ok", "error2", "ok", "error4")));
        JavaRDD<String> errors = rdd.filter(new Contains("ok"));
        errors.collect().forEach(System.out::println);
    }
}


class ContainError implements Function<String, Boolean> {
    //一定要放在主类外面，因为主类并没有实现Serializable接口
    @Override
    public Boolean call(String s) {
        return s.contains("error");
    }
}

class Contains implements Function<String,Boolean>{
    private String query;
    public Contains(String query){
        this.query=query;
    }
    @Override
    public Boolean call(String s) throws Exception {
        return s.contains(query);
    }
}


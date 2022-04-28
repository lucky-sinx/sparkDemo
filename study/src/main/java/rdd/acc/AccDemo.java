package rdd.acc;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

public class AccDemo {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<Log> logs=sc.parallelize(Arrays.asList(
            new Log("address1","40m","KK6JKL","shanghai","china"),
            new Log("address2","40m","KK6JKL","hangzhou","china"),
            new Log("address3","40m","KK6JKL","ningno","china"),
            new Log("address4","40m","KK6JKL","shanghai","japan"),
            new Log("address5","40m","KK6JKL","shanghai","canada"),
            new Log("address6","40m","KK6JKL","shanghai","uk")));
    @Test
    public void testAccumulator() {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList(
            "hello11",
            "",
            "hello12",
            "hello13",
            "hello14",
            "hello15"
        ));
        Accumulator<Integer> accumulator = sc.accumulator(0);
        rdd.flatMap((FlatMapFunction<String, String>) s->{
            if(s.equals(""))accumulator.add(1);
            return Arrays.stream(s.split(" ")).iterator();
        }).collect().forEach(System.out::println);
        rdd.flatMap((FlatMapFunction<String, String>) s->{
            if(s.equals(""))accumulator.add(1);
            return Arrays.stream(s.split(" ")).iterator();
        }).collect().forEach(System.out::println);
        System.out.println("Blank lines:" + accumulator.value());
    }

    @Test
    public void testBroadCast(){
        JavaRDD<String> wantCountry = sc.parallelize(Arrays.asList("china", "japan"));
        Broadcast<JavaRDD<String>> javaRDDBroadcast = sc.broadcast(wantCountry);
    }
}
class Log implements Serializable {
    String address;
    String band;
    String callSign;
    String city;
    String country;

    public Log(String address, String band, String callSign, String city, String country) {
        this.address = address;
        this.band = band;
        this.callSign = callSign;
        this.city = city;
        this.country = country;
    }

    public Log() {
    }
}

/**
 * 恶意请求流量类
 */
class Request implements Serializable{
    public static String origin_id = "origin_id";

    public static String asset_id = "asset_id";

    public static String add_type = "add_type";

    public static String asset_name = "asset_name";
}


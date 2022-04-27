package partition;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Partition1 {
    public static SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    public static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) {
        JavaPairRDD<String, String> pairRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("aaa", "123"),
                new Tuple2<String, String>("bbb", "123"),
                new Tuple2<String, String>("ccc", "123"),
                new Tuple2<String, String>("eee", "123"),
                new Tuple2<String, String>("dba", "123")
        ), 3);
        pairRDD.partitionBy(new MyPartitioner());
        pairRDD.saveAsTextFile("data/p1");
    }
}

class MyPartitioner extends Partitioner{

    @Override
    public int numPartitions() {
        //分区数目
        return 3;
    }

    @Override
    public int getPartition(Object key) {
        //返回数据分区的索引，从0开始
        if(key.equals("aaa"))return 1;
        if(key.equals("bbb"))return 1;
        if(key.equals("ccc"))return 2;
        if(key.equals("eee"))return 2;
        return 0;
    }
}

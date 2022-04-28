package rdd.framework.dao;

import rdd.framework.util.EnvUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCountDao {
    public JavaRDD<String> readFile(String path){
        JavaSparkContext sparkContext = EnvUtil.take();
        JavaRDD<String> lines = sparkContext.textFile(path).cache();
        JavaSparkContext javaSparkContext = EnvUtil.take();
        return lines;
    }
}

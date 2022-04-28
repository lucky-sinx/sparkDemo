package rdd.framework.service;

import rdd.framework.dao.WordCountDao;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WordCountService {
    private WordCountDao wordCountDao = new WordCountDao();

    /**
     * 数据分析，返回结果给controller
     * @return
     * @param sparkContext
     */
    public List<Tuple2<String, Integer>> dataAnalysis(){
        JavaRDD<String> lines = wordCountDao.readFile("D:\\code\\java\\sparkDemo\\WordCountDemo1\\src\\main\\resources\\1.txt");
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(Pattern.compile(" ").split(s)).iterator());
        JavaPairRDD<String, Integer> wordCntOne = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> wordCounts = wordCntOne.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        return wordCounts.collect();
    }
}

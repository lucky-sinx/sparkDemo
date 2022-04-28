package framework.controller;

import framework.service.WordCountService;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * 控制层
 */
public class WordCountController {
    private WordCountService wordCountService = new WordCountService();

    /**
     * 调度,关于如何处理结果
     * @param sparkContext
     */
    public void dispatch(JavaSparkContext sparkContext){
        List<Tuple2<String, Integer>> tuple2s = wordCountService.dataAnalysis();
        tuple2s.forEach(System.out::println);
    }
}

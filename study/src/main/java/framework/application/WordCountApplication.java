package framework.application;

import framework.controller.WordCountController;
import framework.util.EnvUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class WordCountApplication {
    private WordCountController wordCountController=new WordCountController();
    private SparkConf sparkConf = new SparkConf().setAppName("sparkBoot1").setMaster("local");
    private JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    public static void main(String[] args) {
        WordCountApplication wordCountApplication = new WordCountApplication();
        EnvUtil.put(wordCountApplication.sparkContext);
        wordCountApplication.run();
    }

    private void run() {
        wordCountController.dispatch(sparkContext);
    }

}

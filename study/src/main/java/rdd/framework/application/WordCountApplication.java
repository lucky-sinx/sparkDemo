package rdd.framework.application;

import rdd.framework.controller.WordCountController;
import rdd.framework.util.EnvUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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

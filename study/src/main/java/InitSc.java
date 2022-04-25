import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class InitSc {
    public static void main(String[] args) {
        //集群URL:local表明是在单机单线程，无需连接到集群上
        //应用名：连接到集群后这个值可以在集群管理器的用户界面中来寻找到这个应用
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);

    }
}

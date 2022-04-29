package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class sparkJDBC {
    static SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    static SparkSession spark = SparkSession.builder()
            .appName("sql app")
            .config(conf)
            .getOrCreate();

    @Test
    public void test_jdbc1(){
        Dataset<Row> df = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/mybatis")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "root")
                .option("dbtable", "user")
                .load();
        df.show();

        df.write().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/mybatis")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "root")
                .option("dbtable", "user1")
                .save();
        df.show();
    }

}

package mllib.transformer;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.collection.mutable.WrappedArray;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
public class TransformerDemo {
    static SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    static SparkSession spark = SparkSession.builder()
            .appName("sql app")
            .config(conf)
            .getOrCreate();
    @Test
    public void test_tokenizer(){
        /**
         * 分词器
         */
        List<Row> data = Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(1, "I wish Java could use case classes"),
                RowFactory.create(2, "Logistic,regression,models,are,neat")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = spark.createDataFrame(data, schema);
        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("sentence")
                .setOutputCol("words")
                .setPattern("\\W");
        spark.udf().register("countTokens",(WrappedArray<?> words)->words.size(),DataTypes.IntegerType);

        Dataset<Row> tokenized = tokenizer.transform(dataFrame);
        tokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);

        Dataset<Row> regexTokenized = regexTokenizer.transform(dataFrame);
        regexTokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);
    }
}

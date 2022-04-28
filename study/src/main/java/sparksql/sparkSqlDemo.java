package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import sparksql.dao.Person;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;

public class sparkSqlDemo {
    static SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    static SparkSession spark = SparkSession.builder()
            .appName("sql app")
            .config(conf)
            .getOrCreate();
    static String personFilePath = "D:\\code\\java\\sparkDemo\\study\\src\\main\\resources\\json\\user.json";
    static String personFilePath2 = "D:\\code\\java\\sparkDemo\\study\\src\\main\\resources\\json\\person.txt";

    @Test
    public void test_dataFrame() {
        Dataset<Row> df = spark.read().json(personFilePath);
        df.createOrReplaceTempView("user");

        df.show();

        df.select("name").show();

        df.select(col("name"), col("age").plus(100)).show();

        df.filter(col("age").gt(21)).show();
    }

    @Test
    public void test_sql() {
        Dataset<Row> df = spark.read().json(personFilePath);
        df.createOrReplaceTempView("user");
        //sql
        Dataset<Row> allUser = spark.sql("select * from user order by age desc");
        allUser.show();

        spark.sql("select name, age+10 as age from user").show();
    }

    @Test
    public void test_datasets() {
        //1.通过实例转化
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        //Create JavaBean Encoder for Person
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> personDataset = spark.createDataset(Arrays.asList(person), personEncoder);
        personDataset.show();

        //2.普通类型的DataSets
        Encoder<Long> longEncoder = Encoders.LONG();
        Dataset<Long> longDataset = spark.createDataset(Arrays.asList(1L, 2L, 3L), longEncoder);
        Dataset<Long> longDataset1 = longDataset.map((MapFunction<Long, Long>) value -> value + 1L, longEncoder);
        Long[] collect = (Long[]) longDataset1.collect();
        longDataset1.show();

        //3.文件映射到DataSets
        Dataset<Person> personDataset1 = spark.read().json(personFilePath).as(personEncoder);
        personDataset1.show();

    }

    @Test
    public void test_rdd2datasets() {
        //首先创建一个personRDD
        JavaRDD<Person> personRDD = spark.read().textFile(personFilePath2).javaRDD().map((Function<String, Person>) line -> {
            String[] strings = line.split(",");
            Person person = new Person();
            person.setName(strings[0]);
            person.setAge(Integer.parseInt(strings[1].trim()));
            return person;
        });

        // RDD=>DataFrame
        Dataset<Row> personDataFrame = spark.createDataFrame(personRDD, Person.class);
        RDD<Row> personRDD2 = personDataFrame.rdd();

        personDataFrame.createOrReplaceTempView("people");
        Dataset<Row> teenagerDF = spark.sql("select name from people where age between 13 and 19");
        teenagerDF.show();

        // 按照index从dataframe的Row中获取数据
        Dataset<String> teenagerNameDF = teenagerDF.map((MapFunction<Row, String>) row -> {
            return "name: " + row.getString(0);
        }, Encoders.STRING());
        teenagerNameDF.show();

        //按照fieldName获取
        personDataFrame.map((MapFunction<Row, String>) row -> {
            return "name: " + row.<String>getAs("name");
        }, Encoders.STRING());
        personDataFrame.show();

    }

    @Test
    public void test_structType(){
        //通过变量来解析获取不同的dataframe
        JavaRDD<String> peopleRDD = spark.sparkContext().textFile(personFilePath2, 1).toJavaRDD();
        //peopleRDD.collect().forEach(System.out::println);
        String schemaString="name age";

        ArrayList<StructField> fields = new ArrayList<>();
        for (String s : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(s, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        //
        JavaRDD<Row> rowJavaRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] strings = record.split(",");
            return RowFactory.create(strings[0], strings[1].trim());
        });
        Dataset<Row> peopleDF = spark.createDataFrame(rowJavaRDD, schema);
        peopleDF.createOrReplaceTempView("people");
        Dataset<Row> results = spark.sql("select name from people");
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
    }
}

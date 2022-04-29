package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.ArrayList;

public class sparkUDF {
    static SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    static SparkSession spark = SparkSession.builder()
            .appName("sql app")
            .config(conf)
            .getOrCreate();
    static String personFilePath = "D:\\code\\java\\sparkDemo\\study\\src\\main\\resources\\json\\user.json";
    static String personFilePath2 = "D:\\code\\java\\sparkDemo\\study\\src\\main\\resources\\json\\person.txt";

    @Test
    public void test_udf1(){
        /**
         * 不管用的方式
         */
        Dataset<Row> df = spark.read().json(personFilePath);
        df.createOrReplaceTempView("user");

        spark.sql("select age,'name:'+name from user").show();
    }

    @Test
    public void test_udf2(){
        Dataset<Row> df = spark.read().json(personFilePath);
        df.createOrReplaceTempView("user");

        spark.udf().register("prefixName",(String name)->{
            return "Name:"+name;
        }, DataTypes.StringType);

        spark.sql("select age,prefixName(name) as pName from user").show();
        spark.sql("select avg(age) from user").show();
    }

    @Test
    public void test_udaf1(){
        Dataset<Row> df = spark.read().json(personFilePath);
        df.createOrReplaceTempView("user");

        spark.udf().register("ageAvg",new MyAvgUDAF1());
        spark.sql("select ageAvg(age) from user").show();
    }

    @Test
    public void test_udaf2(){
        Dataset<Row> df = spark.read().json(personFilePath);
        df.createOrReplaceTempView("user");

        spark.udf().register("ageAvg",functions.udaf(new MyAvgUDAF2(),Encoders.LONG()));
        spark.sql("select ageAvg(age) from user").show();
    }
}

class MyAvgUDAF1 extends UserDefinedAggregateFunction{

    @Override
    public StructType inputSchema() {
        //输入数据的结构
        ArrayList<StructField> fields =new ArrayList<>();
        fields.add(DataTypes.createStructField("age",DataTypes.LongType,true));
        return DataTypes.createStructType(fields);
    }

    @Override
    public StructType bufferSchema() {
        //缓冲区结构，用于计算的临时数据
        ArrayList<StructField> fields =new ArrayList<>();
        fields.add(DataTypes.createStructField("total",DataTypes.LongType,true));
        fields.add(DataTypes.createStructField("count",DataTypes.LongType,true));
        return DataTypes.createStructType(fields);
    }

    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    @Override
    public boolean deterministic() {
        //函数的稳定性
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        //缓冲区初始化
        buffer.update(0,0L);
        buffer.update(1,0L);

    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if(!input.isNullAt(0)){
            Long updateSum=buffer.getLong(0)+input.getLong(0);
            Long updateCnt=buffer.getLong(1)+1L;
            buffer.update(0,updateSum);
            buffer.update(1,updateCnt);
        }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0));
        buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1));
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getLong(0)*1.0/buffer.getLong(1);
    }
}

class MyAvgUDAF2 extends Aggregator<Long, AvgBuffer,Double>{

    @Override
    public AvgBuffer zero() {
        return new AvgBuffer(0L,0L);
    }

    @Override
    public AvgBuffer reduce(AvgBuffer b, Long a) {
        b.sum+=a;
        b.cnt++;
        return b;
    }

    @Override
    public AvgBuffer merge(AvgBuffer b1, AvgBuffer b2) {
        b1.cnt+=b2.cnt;b1.sum+=b2.sum;
        return b1;
    }

    @Override
    public Double finish(AvgBuffer reduction) {
        return reduction.sum*1.0/reduction.cnt;
    }

    @Override
    public Encoder<AvgBuffer> bufferEncoder() {
        //缓冲取得编码操作，用于网络传输
        return Encoders.javaSerialization(AvgBuffer.class);
    }

    @Override
    public Encoder<Double> outputEncoder() {
        //输出的编码操作，用于网络传输
        return Encoders.DOUBLE();
    }
}
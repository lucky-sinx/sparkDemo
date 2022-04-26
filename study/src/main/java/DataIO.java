import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import util.ReadJson;
import util.WriteJson;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class DataIO {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
    JavaSparkContext sc = new JavaSparkContext(conf);
    @Test
    public void testSaveJson(){
        JavaRDD<Person> rdd = sc.parallelize(Arrays.asList(
                new Person("name1",1),
                new Person("name2",2),
                new Person("name3",3),
                new Person("name4",4),
                new Person("name5",5)
        ));
        rdd.mapPartitions(new WriteJson<Person>()).saveAsTextFile("test2.json");
    }
    @Test
    public void testReadJson(){
        JavaRDD<String> input = sc.textFile("test2.json");
        JavaRDD<Person> personJavaRDD = input.mapPartitions(new ReadPerson());
        //input.collect().forEach(System.out::println);
        personJavaRDD.collect().forEach(System.out::println);
    }

}

class Person implements Serializable{
    String name;
    int age;

    public Person() {
    }

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }
}

class ReadPerson implements FlatMapFunction<Iterator<String>,Person> {
    @Override
    public Iterator<Person> call(Iterator<String> stringIterator) throws Exception {
        ArrayList<Person > tArrayList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        while (stringIterator.hasNext()){
            String next = stringIterator.next();
            try{
                boolean add = tArrayList.add(objectMapper.readValue(next,Person.class));
            }catch (Exception e){
                System.out.println(e);
            }
        }
        return tArrayList.iterator();
    }
}

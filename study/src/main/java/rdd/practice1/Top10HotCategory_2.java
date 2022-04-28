package rdd.practice1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 使用自定义累加器
 */
public class Top10HotCategory_2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = sc.textFile("D:\\code\\java\\sparkDemo\\study\\src\\main\\java\\rdd.practice1\\user_visit_action.txt");

        ////1.切分原始数据
        //JavaRDD<String[]> dataRDD = fileRDD.map((Function<String, String[]>) s -> s.split("_"));

        //1.声明累加器并注册
        MyAccumulatorPractise myAccumulatorPractise = new MyAccumulatorPractise();
        sc.sc().register(myAccumulatorPractise, "myAcc");
        //2.开始累加
        fileRDD.foreach((VoidFunction<String>) line -> myAccumulatorPractise.add(line));
        Map<String, int[]> map = myAccumulatorPractise.value();

        map.forEach((BiConsumer<? super String, ? super int[]>) (key, value) -> {
            System.out.printf("%s:[%d,%d,%d]\n", key, value[0], value[1], value[2]);
        });

        List<Map.Entry<String, int[]>> list = map.entrySet().stream().sorted((e1, e2) -> {
            int[] o1 = e1.getValue(), o2 = e2.getValue();
            if (o1[0] != o2[0]) return o2[0] - o1[0];
            else if (o1[1] != o2[1]) return o2[1] - o1[1];
            else return o2[2] - o1[2];
        }).collect(Collectors.toList());
        System.out.println("*********************");

        list.forEach(new Consumer<Map.Entry<String, int[]>>() {
            @Override
            public void accept(Map.Entry<String, int[]> stringEntry) {
                String key = stringEntry.getKey();
                int[] value = stringEntry.getValue();
                System.out.printf("%s:[%d,%d,%d]\n", key, value[0], value[1], value[2]);
            }
        });
    }
}

class MyAccumulatorPractise extends AccumulatorV2<String, Map<String, int[]>> {
    Map<String, int[]> map = new HashMap<>();

    @Override
    public boolean isZero() {
        return map.isEmpty();
    }

    @Override
    public AccumulatorV2<String, Map<String, int[]>> copy() {
        return new MyAccumulatorPractise();
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void add(String v) {
        String[] strings = v.split("_");
        if (!strings[6].equals("-1")) {
            add(strings[6], 0);
        } else if (!strings[8].equals("null")) {
            Arrays.stream(strings[8].split(",")).forEach((Consumer<? super String>) s -> {
                add(s, 1);
            });
        } else if (!strings[10].equals("null")) {
            Arrays.stream(strings[10].split(",")).forEach((Consumer<? super String>) s -> {
                add(s, 2);
            });
        }
    }

    public void add(String v, int index) {
        if (!map.containsKey(v)) {
            map.put(v, new int[]{0, 0, 0});
        }
        map.get(v)[index]++;
    }

    @Override
    public void merge(AccumulatorV2<String, Map<String, int[]>> other) {
        Map<String, int[]> map1 = this.map;
        Map<String, int[]> map2 = other.value();
        map2.forEach(new BiConsumer<String, int[]>() {
            @Override
            public void accept(String s, int[] ints) {
                if (!map1.containsKey(s)) {
                    map1.put(s, new int[]{0, 0, 0});
                }
                for (int i = 0; i < 3; i++) map1.get(s)[i] += ints[i];
            }
        });
    }

    @Override
    public Map<String, int[]> value() {
        return map;
    }
}

package rdd.practice1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.util.function.Consumer;

public class PageJumpAnalysis {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRDD = sc.textFile("D:\\code\\java\\sparkDemo\\study\\src\\main\\java\\rdd.practice1\\user_visit_action.txt");
        JavaRDD<String[]> dataRDD = fileRDD.map((Function<String, String[]>) s -> s.split("_"));
        dataRDD.cache();

        //1.计算分母,无需排序，直接计算wordCount即可
        JavaRDD<ActionData> actionRDD = dataRDD.map((Function<String[], ActionData>) datas -> {
            return new ActionData(
                    datas[0],
                    Long.valueOf(datas[1]),
                    datas[2],
                    Long.valueOf(datas[3]),
                    datas[4],
                    datas[5],
                    Long.valueOf(datas[6]),
                    Long.valueOf(datas[7]),
                    datas[8],
                    datas[9],
                    datas[10],
                    datas[11],
                    Long.valueOf(datas[12])
            );
        });
        JavaPairRDD<Long, Integer> pageCnt = actionRDD.mapToPair((PairFunction<ActionData, Long, Integer>) action -> {
            return new Tuple2<>(action.pageId, 1);
        }).reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> (x + y));

        Map<Long, Integer> map = pageCnt.collectAsMap();
        //List<Tuple2<Long, Integer>> collect = (List<Tuple2<Long, Integer>>) longIntegerMap;

        //2.计算分子

        //根据session进行分组
        JavaPairRDD<String, Iterable<ActionData>> sessionRDD = actionRDD.groupBy(actionData -> actionData.sessionID);
        List<Tuple2<String, Iterable<ActionData>>> sessionResult = sessionRDD.collect();
        //然后按照时间进行排序
        JavaPairRDD<String, Iterable<ActionData>> sortSessionRDD = sessionRDD.mapValues((Function<Iterable<ActionData>, Iterable<ActionData>>) iterable -> {
            Iterator<ActionData> iterator = iterable.iterator();
            List<ActionData> list = new LinkedList<>();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            list.sort(new Comparator<ActionData>() {
                @Override
                public int compare(ActionData o1, ActionData o2) {
                    return o1.actionTime.compareTo(o2.actionTime);
                }
            });
            return list;
        });
        List<Tuple2<String, Iterable<ActionData>>> sortSessionResult = sortSessionRDD.collect();
        //映射到pageID
        JavaPairRDD<String, Iterable<Long[]>> pageJumpRDD = sortSessionRDD.mapValues((Function<Iterable<ActionData>, Iterable<Long[]>>) iterable -> {
            Iterator<ActionData> iterator = iterable.iterator();
            List<Long> list = new LinkedList<>();
            while (iterator.hasNext()) {
                list.add(iterator.next().pageId);
            }
            List<Long[]> res=new LinkedList<>();
            for(int i=0;i<list.size()-1;i++){
                res.add(new Long[]{list.get(i),list.get(i+1)});
            }
            return res;
        });
        List<Tuple2<String, Iterable<Long[]>>> sortSessionPageIds = pageJumpRDD.collect();
        //计算每种跳转的次数
        JavaPairRDD<Tuple2<Long, Long>, Integer> jumpTimesRDD = pageJumpRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Long[]>>, Tuple2<Long, Long>, Integer>() {
            @Override
            public Iterator<Tuple2<Tuple2<Long, Long>, Integer>> call(Tuple2<String, Iterable<Long[]>> stringIterableTuple2) throws Exception {
                Iterator<Long[]> iterator = stringIterableTuple2._2().iterator();
                List<Long[]> list = new LinkedList<>();
                iterator.forEachRemaining((Consumer<? super Long[]>) jump -> {
                    list.add(jump);
                });
                List<Tuple2<Tuple2<Long, Long>, Integer>> res = new LinkedList<>();
                list.forEach((Consumer<? super Long[]>) jump -> {
                    res.add(new Tuple2<>(new Tuple2(jump[0], jump[1]), 1));
                });
                return res.iterator();
            }
        }).reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> (x + y));

        List<Tuple2<Tuple2<Long, Long>, Integer>> jumpTimes = jumpTimesRDD.collect();

        //3.计算单跳转换率（分子/分母）
        jumpTimes.forEach((Consumer<? super Tuple2<Tuple2<Long, Long>, Integer>>) tuple->{
            Long pageId1 = tuple._1()._1();
            Long pageId2 = tuple._1()._2();
            int a=tuple._2();
            Integer b = map.getOrDefault(tuple._1()._1(), 1);
            System.out.printf("page %d -> %d : %.7f\n",pageId1,pageId2,a*1.0f/b);
        });
        int a=0;

    }
}

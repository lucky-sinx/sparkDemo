package sparkStreaming.practise;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class MockData {
    public static void main(String[] args) {
        //new Propertie;
        //new KafkaProducer<>()
        //向kafka生成数据
        while (true){
            mockData().forEach(
                    s->{

                    }
            );
        }

    }

    public static List<String> mockData(){
        List<String> resList=new LinkedList<>();
        List<String> areaList=new LinkedList<>(Arrays.asList("华东","华中","华南"));
        List<String> cityList=new LinkedList<>(Arrays.asList("上海","嘿嘿","深圳"));
        for (int i = 0; i < 30; i++) {
            Random random = new Random();
            String area = areaList.get(random.nextInt(areaList.size()));
            String city = cityList.get(random.nextInt(cityList.size()));
            int userId = random.nextInt(6);
            int adId = random.nextInt(6);
            resList.add(String.format("%ld %s %s %d %d",System.currentTimeMillis(),area,city,userId,adId));
        }
        return resList;
    }
}

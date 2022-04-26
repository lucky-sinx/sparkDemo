package util;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 在Java中保存Json
 * @param <T>
 */
public class WriteJson<T> implements FlatMapFunction<Iterator<T>,String> {
    @Override
    public Iterator<String> call(Iterator<T> tIterator) throws Exception {
        ArrayList<String> text = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        while(tIterator.hasNext()){
            T t=tIterator.next();
            text.add(objectMapper.writeValueAsString(t));
        }
        return text.iterator();

    }
}
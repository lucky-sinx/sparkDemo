package rdd.util;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 在Java中读取Json
 * @param <T>
 */
public class ReadJson<T> implements FlatMapFunction<Iterator<String>,T> {
    public static Class<?> getActualArgument(Class<?> clazz ){
        Class<?> e=null;
        Type genericSuperclass = clazz.getGenericSuperclass();
        if(genericSuperclass instanceof ParameterizedType){
            Type[] actualTypeArguments = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
            e=(Class<?>) actualTypeArguments[0];
        }
        return e;
    }


    @Override
    public Iterator<T> call(Iterator<String> stringIterator) throws Exception {
        ArrayList<T> tArrayList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        while (stringIterator.hasNext()){
            String next = stringIterator.next();
            try{
                Class<T> actualTypeArgument = (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
                boolean add = tArrayList.add(objectMapper.readValue(next,actualTypeArgument));
            }catch (Exception e){
                System.out.println(e);
            }
        }
        return tArrayList.iterator();
    }
}



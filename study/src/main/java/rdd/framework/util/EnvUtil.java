package rdd.framework.util;

import org.apache.spark.api.java.JavaSparkContext;

public class EnvUtil {
    private static ThreadLocal threadLocal =new ThreadLocal<JavaSparkContext>();

    public static void put(JavaSparkContext sc){
        threadLocal.set(sc);
    }

    public static JavaSparkContext take(){
        return (JavaSparkContext) threadLocal.get();
    }

    public static void clear(){
        threadLocal.remove();
    }
}

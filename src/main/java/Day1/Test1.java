package Day1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class Test1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        //首先继续flatmap操作
        JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return new ArrayList<String>(Arrays.asList(s.split(" "))).iterator();
            }
        });
        //这里为什么使用mapToPair  而不是 map
        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return Tuple2.apply(s, 1);

            }
        });
        //对值继续sum
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //排序,但是只能安key来,反转
        JavaPairRDD<Integer, String> mapToPair1 = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();


            }
        });
        //排序
        JavaPairRDD<Integer, String> sortByKey = mapToPair1.sortByKey(false);
        //再次反转
        JavaPairRDD<String, Integer> mapToPair2 = sortByKey.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });

        mapToPair2.saveAsTextFile(args[1]);
        sc.stop();
    }
}

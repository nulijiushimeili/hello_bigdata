package java_spark;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


/**
 * java spark word count
 * <p>
 * create by nulijiushimeili on 2018-08-03
 */
public class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
//        if (args.length < 1) {
//            System.err.println("Usage:JavaWordCount <file>");
//            System.exit(1);
//        }

        String filePath = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\wc.input";

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(conf);
//        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        JavaRDD<String> lines = ctx.textFile(filePath, 1);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                }
        );

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<?, ?> tuple2 : output) {
            System.out.println(tuple2._1() + "----" + tuple2._2());
        }

        ctx.stop();


    }
}

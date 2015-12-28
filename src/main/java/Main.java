import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) {
        System.out.println("Starting...");

        SparkConf conf = new SparkConf().setAppName("sandbox").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = IntStream.range(0, 1000000).boxed().collect(Collectors.toList());

        JavaRDD<Integer> distData = sc.parallelize(numbers);

        long startTime = System.currentTimeMillis();
        long count = distData.reduce((a, b) -> a + b);
        long endTime = System.currentTimeMillis();

        System.out.println(String.format("Count: %d; Time: %d ms", count, endTime - startTime));
    }
}

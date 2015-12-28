import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sandbox").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SumListOfNumbers(sc);
        NasaHttpLogs(sc);
    }

    /*
     *  Uses reduce to sum a list of numbers together
     */
    private static void SumListOfNumbers(JavaSparkContext sc) {
        List<Integer> numbers = IntStream.range(0, 1000000).boxed().collect(Collectors.toList());
        long expectedSum = numbers.parallelStream().reduce((a, b) -> a + b).get();

        JavaRDD<Integer> distData = sc.parallelize(numbers);

        long startTime = System.currentTimeMillis();
        long actualSum = distData.reduce((a, b) -> a + b);
        long endTime = System.currentTimeMillis();

        System.out.println(
                String.format(
                        "Count: %d; Time: %d ms; Accurate: %b",
                        actualSum,
                        endTime - startTime,
                        expectedSum == actualSum
                )
        );
    }

    private static void NasaHttpLogs(JavaSparkContext sc) {
        JavaRDD<String> logs = sc.textFile(".data/access_log_Jul95");
        System.out.println(String.format("Count: %d", logs.count()));
    }
}

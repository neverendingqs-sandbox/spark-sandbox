import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) throws Exception {
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

    /*
     *  Uses various RDD transformations on web logs to derive different values
     */
    private static void NasaHttpLogs(JavaSparkContext sc) {
        final String logPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+)\\s*(\\S*)\" (\\d{3}) (\\S+)";

        JavaRDD<String> logs = sc.textFile(".data/access_log_Jul95");

        long lineCount = logs.count();
        System.out.println(String.format("lineCount: %d", lineCount));
        // lineCount: 1891715

        JavaRDD<String> validLogs = logs.filter(x ->
                Pattern.compile(logPattern)
                        .matcher(x)
                        .matches()
                )
                .cache();
        long validLogsLineCount = validLogs.count();
        System.out.println(String.format("validLogsLineCount: %d", validLogsLineCount));
        // validLogsLineCount: 1890851

        long num200Responses = validLogs.map(x -> {
                Matcher matcher = Pattern.compile(logPattern).matcher(x);
                if (matcher.matches()) {
                    String responseCode = matcher.group(8);
                    return responseCode;
                }
                else {
                    throw new Exception("All logs should be valid.");
                }
            })
            .map(Integer::parseInt)
            .filter(x -> x == 200)
            .count();
        System.out.println(String.format("num200Responses: %d", num200Responses));
        // num200Responses: 1700743
    }
}

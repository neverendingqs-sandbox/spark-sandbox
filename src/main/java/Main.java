import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("sandbox").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        CoreExamples(sc);
    }

    private static void CoreExamples(JavaSparkContext sc) {
        CoreExamples coreExamples = new CoreExamples(sc);

        coreExamples.SumListOfNumbers();
        coreExamples.NasaHttpLogs();
    }
}

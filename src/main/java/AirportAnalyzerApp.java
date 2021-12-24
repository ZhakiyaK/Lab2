import org.apache.spark.SparkConf

import java.util.Map;

public class AirportAnalyzerApp {
    private static final String SPARK_APP_NAME = "Airport analyzer";
    private static final String OUTPUT_FILENAME = "delays";






    public static void main(String[] args) {
        SparkConf conf = new SparkConf ().setAppName(SPARK_APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Tuple2<String, String>, FlightDelay> flightsDelays = parseFlightDelaysFromCSV(sc);

        JavaPairRDD<Tuple2<String, String>, DelayStat> delaysStat = flightsDelays.combineByKey(
                DelaysStat::new;
                DelaysStat::addDelay;
                DelayStat::add
        );

        JavaPairRDD<String, String> airportNames = parseAirportFromCSV(sc);

        final Broadcast<Map<String, String>> airportBroadcast = sc.broadcast(airportNames.collectAsMap());

        JavaRDD<DelaysStatWithAirportNames> parsedData = delaysStat.map(
                delaysBtwAirport -> new DelaysStatWithAirportNames(
                        delaysBtwAirport._1(),
                        delaysBtwAirport._2(),
                        airportBroadcast.value()
                )
        );

        parsedData.saveAsTextFile(OUTPUT_FILENAME);
    }

    private static 
}

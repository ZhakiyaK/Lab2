

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class AirportAnalyzerApp {
    private static final String SPARK_APP_NAME = "Airport analyzer";
    private static final String HDFS_PATH_TO_FLIGHTS = "airport.csv";
    private static final String HDFS_PATH_TO_AIRPORTS = "airports.csv";
    private static final String FLIGHTS_FILE_FIRST_LINE_PREFIX = "\"";
    private static final String AIRPORTS_FILE_FIRST_LINE_PREFIX = "C";
    private static final String DATA_SEPERATOR = ",";
    private static final String OUTPUT_FILENAME = "delays";
    private static final int    AIRPORT_ID_INDEX = 0;
    private static final int    AIRPORT_NAME_INDEX = 1;



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

    private static JavaRDD<String> readDataFromCSV(JavaSparkContext sc,
                                                   final String path,
                                                   final String firstLinePrefix) {
        JavaRDD<String> data = sc.textFile(path);
        return data.filter(line -> !line.startsWith(firstLinePrefix));
    }

    private static JavaPairRDD<Tuple2<String,String>, FlightDelay> parseFlightsDelaysFromCSV(JavaSparkContext sc) {
        return readDataFromCSV(sc, HDFS_PATH_TO_FLIGHTS, FLIGHTS_FILE_FIRST_LINE_PREFIX).mapToPair(
               airport -> {
                   String[] airportData = airport.split(DATA_SEPERATOR, 2);
                   return new Tuple2<>(
                           FlightDelay.deleteDoubleQuotes(
                                   airportData[AIRPORT_ID_INDEX]
                           ),
                           FlightDelay.deleteDoubleQuotes(
                                   airportData[AIRPORT_NAME_INDEX]
                           )
                   );
               }
        );
    }
}

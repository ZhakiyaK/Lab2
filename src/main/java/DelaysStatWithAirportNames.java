import scala.Tuple2;

public class DelaysStatWithAirportNames extends DelaysStat {
    private static final String FLOAT_STRING_FORMAT = "%.2f";
    private static final int    PERCENT_CONVERSION_CONSTANT = 100;

    private final String        departureAirportName;
    private final String        destinationAirportName;



    public DelaysStatWithAirportNames(Tuple2<String, String> airportNames, DelaysStat delaysStatSrc, Map<String, String> airportName) {
        super (
                delaysStatSrc.getMaxDelay(),
                delaysStatSrc.getFlightsCount(),
                delaysStatSrc.getDelayedCount(),
                delaysStatSrc.getCancelledCount(),
        );

        this.departureAirportName = airportName.get(airportName._1());
        this.destinationAirportName = airportName.get(airportName._2());
    }

    @Override
    public String toString() {
        String percentOfCancelledFlights = percentage(getCancelledCount(), getFlightsCount());
        String percentOfDelayedFlights = percentage(getDelayedCount(), getFlightsCount());
        return departureAirportName + " -> " + destinationAirportName +
                "\nMax delay: " + getMaxDelay() +
                "\n" + percentOfCancelledFlights + "% flights were cancelled" +
                "\n" + percentOfDelayedFlights + "% flights were delayed\n";
    }

    private static String percentage(float numerator, int denominator) {
        return String.format(FLOAT_STRING_FORMAT, numerator / denominator * PERCENT_CONVERSION_CONSTANT);
    }
}

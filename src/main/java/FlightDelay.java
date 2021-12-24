import java.io.Serializable;

public class FlightDelay implements Serializable {
    private static final int    CANCELLED_STATUS_INDEX = 1;
    private static final int    DELAY_DURATION_INDEX = 2;
    private static final int    DEPARTURE_AIRPORT_ID_INDEX = 3;
    private static final int    DESTINATION_AIRPORT_ID_INDEX = 4;
    private static final String DOUBLE_QUOTES_REGEX = "\"";
    private static final String EMPTY_STRING = "";

    private final boolean cancelledStatus;
    private float         delayDuration;

    protected FlightDelay(String[] flightData) {
        String cancelledStatus = flightData[CANCELLED_STATUS_INDEX];

        if (!cancelledStatus.isEmpty()) {
            this.cancelledStatus = false;
            this.delayDuration = Float.parseFloat(
                    flightData[DELAY_DURATION_INDEX]
            );
        }
        else {
            this.cancelledStatus = true;
        }
    }

    protected boolean getCancelledStatus() {
        return this.cancelledStatus;
    }

    protected float getDelayDuration() {
        return this.delayDuration;
    }

    protected static Tuple2<String, String> makePairOfDepartureAndDestinationAirportIDs(String[] flightData) {
        return new Tuple2<>(
                flightData[DEPARTURE_AIRPORT_ID_INDEX],
                flightData[DESTINATION_AIRPORT_ID_INDEX]
        );
    }

    protected static String deleteDoubleQuotes(String s) {
        return s.replace(DOUBLE_QUOTES_REGEX, EMPTY_STRING);
    }
}

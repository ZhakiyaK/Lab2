import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class AirportIDGroupingComparator extends WritableComparator {

    public AirportIDGroupingComparator() {
        super(AirportWritableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AirportWritableComparable id1 = (AirportWritableComparable) a;
        AirportWritableComparable id2 = (AirportWritableComparable) b;
        return id1
                .getAirportID()
                .compareTo(id2
                .getAirportID()
                );
    }
}

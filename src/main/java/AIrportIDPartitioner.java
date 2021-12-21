import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AIrportIDPartitioner extends Partitioner <AirportWritableComparable, Text> {

    @Override
    public int getPartition(AirportWritableComparable airportWritableComparable, Text text, int numReduceTasks) {
        return Math.abs(
                airportWritableComparable
                .getAirportID()
                .hashCode()
        ) % numReduceTasks;
    }
}

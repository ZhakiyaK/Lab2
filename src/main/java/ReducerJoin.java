import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoin extends Reducer<AirportIDGroupingComparator, Text, Text, Text> {
    private static final String FLOAT_NUMBER_REG_EX = "^\\d+\\.\\d+$";

    @Override
    protected void reduce(AirportWritableComparable key, Iterable<Text> values, Context context) throws 
}

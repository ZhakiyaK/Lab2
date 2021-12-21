import jdk.internal.module.ModuleLoaderMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class FlightJoinMapper extends ModuleLoaderMap.Mapper <LongWritable, Text, AirportWritableComparable> {
}

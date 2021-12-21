import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class AirportJoinMapper extends Mapper<LongWritable, Text, AirportIDWritableComparable, Text>
{   private static final String SEPERATOR = ",";
    private static final String EMPTY_STRING = "";
    private  static final String CSV_COLOUMN_NAME = "Code";
    private 
    private static final int AIRPORT_ID_INDEX = 0;
    private static final int AIRPORT_NAME_INDEX = 1;

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException, NumberFormatException {
        String[] values = value
                .toString()
                .split(SEPERATOR, LIMIT_SEPERATOR)
    }
}

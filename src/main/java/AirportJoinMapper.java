import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class AirportJoinMapper extends Mapper<LongWritable, Text, AirportIDWritableComparable, Text>
{   private static final String SEPERATOR = ",";
    private static final String EMPTY_STRING = "";
    private static final String CSV_COLOUMN_NAME = "Code";
    private static final String DOUBLE_QUOTES_REG_EX = "\"";
    private static final int AIRPORT_ID_INDEX = 0;
    private static final int LIMIT_SEPERATOR = 2;
    private static final int AIRPORT_NAME_INDEX = 1;
    private static final int DATASET_INDICATOR = 0;

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException, NumberFormatException {
        String[] values = value
                .toString()
                .split(SEPERATOR, LIMIT_SEPERATOR);

        String airportIdString = removeDoubleQotesFromString(value[AIRPORT_ID_INDEX]);
        String airportName = removeDoubleQotesFromString(value[AIRPORT_NAME_INDEX]);
        if (!airportIdString.equals(CSV_COLOUMN_NAME)) {
            int airportID = Integer.parseInt(airportIdString);
            context.write(
                    new AirportIDWritableComparable(
                            new IntWritable(airportID),
                            new IntWritable(DATASET_INDICATOR)
                    ),
                    new Text(airportName)
            );

        }
    }
    private static String removeDoubleQotesFromString(final String data) {
        return data
                .replaceAll(DOUBLE_QUOTES_REG_EX, EMPTY_STRING);
    }
}

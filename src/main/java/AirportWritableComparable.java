import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritableComparable implements WritableComparable<AirportWritableComparable>  {
    private final IntWritable airportID;
    private final IntWritable datasetIndicator;

    public AirportWritableComparable() {
        airportID = new IntWritable(0);
        datasetIndicator = new IntWritable(0);
    }

    public AirportWritableComparable(IntWritable airportID, IntWritable datasetIndicator) {
        this.airportID = airportID;
        this.datasetIndicator = datasetIndicator;
    }

    protected IntWritable getAirportID() {
        return this.airportID;
    }

    protected IntWritable getDatasetIndicator() {
        return this.datasetIndicator;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        airportID.write(dataOutput);
        datasetIndicator.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportID.readFields(dataInput);
        datasetIndicator.readFields(dataInput);
    }

    @Override
    public int compareTo(AirportWritableComparable o) {
        int resultOfCompareAirportID = this.getAirportID()
                .compareTo(
                        o.getAirportID()
                );
        if (resultOfCompareAirportID == 0) {
            return this.getDatasetIndicator()
                    .compareTo(
                            o.getDatasetIndicator()
                    );
        } else {
            return  resultOfCompareAirportID;
        }
    }
}


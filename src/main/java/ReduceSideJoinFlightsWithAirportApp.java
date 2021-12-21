import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceSideJoinFlightsWithAirportApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ReduceSideJoinFlightsAirportApp <inputFlights path> <inputAirport path> <output path> ");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(ReduceSideJoinFlightsWithAirportApp.class);
        job.setJobName("Reduce side join flights with airports");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportJoinMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setPartitionerClass(AirportIDPartitioner.class);
        job.setGroupingComparatorClass(AirportIDGroupingComparator.class);
        job.setReducerClass(ReducerJoin.class);
        job.setMapOutputKeyClass(AirportWritableComparable.class);
        job.setOutputKeyClass();KeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
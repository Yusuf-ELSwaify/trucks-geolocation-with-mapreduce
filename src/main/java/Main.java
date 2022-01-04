import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.time.Duration;
import java.time.Instant;
public class Main {
    public static void main(String[] args)  throws Exception{

        //args[0] trucks csv
        //args[1] geolocation csv
        //args[2] output
        Instant begin = Instant.now();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        FileSystem fs = FileSystem.get(conf);
        String output =args[2];
        boolean exists = fs.exists(new Path(output));
        if (exists) {
            fs.delete(new Path(output), true);
        }
        job.setJarByClass(Main.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperTrucks.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperGeo.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        Duration timeElapsed = Duration.between(begin, Instant.now());
        System.out.println("Finished in "+ timeElapsed.toMillis()+" millisecond");
    }
}

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class MapperTrucks extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String ID = value.toString().split(",",3)[1];
        context.write(new Text(ID), new Text(":"+value));
    }
}

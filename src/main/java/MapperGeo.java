import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;




public class MapperGeo extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String ID = fields[0];
        if(fields[5].equals("San Diego"))
            context.write(new Text(ID), value);
    }
}


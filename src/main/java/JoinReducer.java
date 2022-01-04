import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReducer extends Reducer<Text,Text,Text,Text> {
    List<String> trucks;
    List<String> locations;
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        trucks = new ArrayList<String>();
        locations = new ArrayList<String>();
        for (Text value: values) {
            String stringValue = value.toString();
            if(stringValue.startsWith(":")) 
                trucks.add(stringValue);
            else
                locations.add(stringValue);
        }
        for (String location: locations) {
            for (String truck: trucks) {
                context.write( new Text(location+truck),new Text(""));
            }
        }

    }

}

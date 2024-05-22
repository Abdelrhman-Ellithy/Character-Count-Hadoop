import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CCMapper extends MapReduceBase implements Mapper<LongWritable,
Text, Text, IntWritable> {

// Map function
public void map(LongWritable key, Text value, OutputCollector<Text,
IntWritable> output, Reporter rep) throws IOException
{
    String line = value.toString();
    
    // Splitting the line on spaces
    for (String word : line.split(" "))
    {
        for (int i = 0; i < word.length(); i++)
        {
            char character = word.charAt(i);
            output.collect(new Text(String.valueOf(character)), new IntWritable(1));
        }
    }
}
}


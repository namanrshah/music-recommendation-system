package proj.mrs.mappers;

import proj.mrs.utils.Constants;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author namanrs
 */
public class MapperUserSongs extends
        Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
        int playCount = Integer.parseInt(splittedValues[2]);
        if (playCount > 2) {
            context.write(new Text(splittedValues[0]), new Text(splittedValues[1]));
        }
    }
}

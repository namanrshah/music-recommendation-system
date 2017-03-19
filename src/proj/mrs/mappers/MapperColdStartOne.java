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
public class MapperColdStartOne extends
        Mapper<LongWritable, Text, Text, Text> {

    //Inputs - UserReco 3 + Actual dataset(Training/whole)
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
        int len = splittedValues.length;
        if (len == 2) {
            context.write(new Text(splittedValues[0]), new Text(Constants.STARTS_WITH.RECO_FOR_COLD_START));
        } else if (len == 3) {
            context.write(new Text(splittedValues[0]), new Text(splittedValues[1] + Constants.SEPARATORS.SONG_PLAY_COUNT_SEPARATOR + splittedValues[2]));
        }
    }
}

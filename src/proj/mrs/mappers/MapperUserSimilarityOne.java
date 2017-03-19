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
public class MapperUserSimilarityOne extends
        Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
//        context.write(new Text(Constants.STARTS_WITH.USER_ID_USER_LISTENED_SONGS + splittedValues[0]), Constants.one);
        if (Integer.parseInt(splittedValues[2]) > 2) {
            context.write(new Text(splittedValues[1]), new Text(splittedValues[0] + Constants.SEPARATORS.USER_PLAY_COUNT_SEPARATOR + splittedValues[2]));
        }
    }
}

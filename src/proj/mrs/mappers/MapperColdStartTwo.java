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
public class MapperColdStartTwo extends
        Mapper<LongWritable, Text, Text, Text> {

    //Inputs - coldStartOne + UserSimilarityOne
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
        String firstSplitValue = splittedValues[0];
        if (firstSplitValue.startsWith(Constants.STARTS_WITH.USERS_WITH_NO_RECO)) {
            firstSplitValue = firstSplitValue.substring(Constants.STARTS_WITH.USERS_WITH_NO_RECO.length());
            context.write(new Text(splittedValues[1]), new Text(Constants.STARTS_WITH.USERS_WITH_NO_RECO + firstSplitValue + Constants.SEPARATORS.USER_PLAY_COUNT_SEPARATOR + splittedValues[2]));
        } else {
            context.write(new Text(firstSplitValue), new Text(splittedValues[1]));
        }
    }
}

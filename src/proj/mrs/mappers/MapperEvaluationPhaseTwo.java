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
public class MapperEvaluationPhaseTwo extends
        Mapper<LongWritable, Text, Text, Text> {

    //Inputs - coldStartOne + UserSimilarityOne
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
        int len = splittedValues.length;
        String userId = splittedValues[0];
        if (len == 2) {
            //reco dataset
            context.write(new Text(userId), new Text(splittedValues[1]));
        } else if (len == 3) {
            //test dataset
            context.write(new Text(userId), new Text(Constants.STARTS_WITH.USER_SONGS + splittedValues[1]));
        }
    }
}

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
public class MapperUserCandidateSongsTwo extends
        Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
        String userId = splittedValues[0];
        if (userId.startsWith(Constants.STARTS_WITH.USER_SONGS)) {
            //user-songs dataset
            userId = userId.substring(Constants.STARTS_WITH.USER_SONGS.length());
            context.write(new Text(userId), new Text(Constants.STARTS_WITH.USER_SONGS_FOR_CANDIDATE_SET_COMPUTATION + splittedValues[1]));
        } else {
            //user-candidate-songs-dataset
            context.write(new Text(userId), new Text(splittedValues[1]));
        }
    }
}

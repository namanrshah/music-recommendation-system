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
public class MapperRecommendationTwo extends
        Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
        String userSongCombo = splittedValues[0];
        if (userSongCombo.startsWith(Constants.STARTS_WITH.CANDIDATE_SONGS_FOR_USER)) {//candidate set
            userSongCombo = userSongCombo.substring(Constants.STARTS_WITH.CANDIDATE_SONGS_FOR_USER.length());
            context.write(new Text(userSongCombo + Constants.SEPARATORS.COMBO_SEPARATOR + splittedValues[1]), new Text(Constants.STARTS_WITH.CANDIDATE_SONGS_FOR_USER));
        } else {//User-songs weight
            context.write(new Text(userSongCombo), new Text(splittedValues[1]));
        }
    }
}

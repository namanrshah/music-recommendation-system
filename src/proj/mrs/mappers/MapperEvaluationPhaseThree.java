package proj.mrs.mappers;

import proj.mrs.utils.Constants;
import proj.mrs.utils.FloatSumAndCount;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author namanrs
 */
public class MapperEvaluationPhaseThree extends
        Mapper<LongWritable, Text, Text, FloatSumAndCount> {

    //Inputs - coldStartOne + UserSimilarityOne
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] splittedValues = valueString.split("\t");
        context.write(new Text(Integer.toString(splittedValues[0].hashCode() % 10)), new FloatSumAndCount(Float.parseFloat(splittedValues[1]), 1));
    }
}

package proj.mrs.jobs;

import proj.mrs.combiners.CombinerEvaluationPhaseThree;
import proj.mrs.combiners.CombinerUserSimilarityTwo;
import proj.mrs.mappers.MapperColdStartAnalysis;
import proj.mrs.mappers.MapperColdStartAnalysisTwo;
import proj.mrs.mappers.MapperColdStartFour;
import proj.mrs.mappers.MapperColdStartOne;
import proj.mrs.mappers.MapperColdStartThree;
import proj.mrs.mappers.MapperColdStartTwo;
import proj.mrs.mappers.MapperEvaluationPhaseOne;
import proj.mrs.mappers.MapperEvaluationPhaseThree;
import proj.mrs.mappers.MapperEvaluationPhaseTwo;
import proj.mrs.mappers.MapperRecommendationOne;
import proj.mrs.mappers.MapperRecommendationThree;
import proj.mrs.mappers.MapperRecommendationTwo;
import proj.mrs.mappers.MapperSongToAvgValue;
import proj.mrs.mappers.MapperTrainTestPartition;
import proj.mrs.mappers.MapperUserCandidateSongsOne;
import proj.mrs.mappers.MapperUserCandidateSongsTwo;
import proj.mrs.mappers.MapperUserSimilarityOne;
import proj.mrs.mappers.MapperUserSimilarityThree;
import proj.mrs.mappers.MapperUserSimilarityTwo;
import proj.mrs.mappers.MapperUserSongs;
import proj.mrs.mappers.MapperUserSongsWithPlayCount;
import proj.mrs.mappers.MapperUserSongsWithoutFiltering;
import proj.mrs.mappers.MapperUserToSongsListenedStats;
import proj.mrs.reducers.ReducerColdStartCandidateSongsTwo;
import proj.mrs.reducers.ReducerColdStartFour;
import proj.mrs.reducers.ReducerColdStartOne;
import proj.mrs.reducers.ReducerColdStartThree;
import proj.mrs.reducers.ReducerColdStartTwo;
import proj.mrs.reducers.ReducerColdStartanalysis;
import proj.mrs.reducers.ReducerColdStartanalysisTwo;
import proj.mrs.reducers.ReducerEvaluationPhaseOne;
import proj.mrs.reducers.ReducerEvaluationPhaseThree;
import proj.mrs.reducers.ReducerEvaluationPhaseTwo;
import proj.mrs.reducers.ReducerRecommendationOne;
import proj.mrs.reducers.ReducerRecommendationThree;
import proj.mrs.reducers.ReducerRecommendationTwo;
import proj.mrs.reducers.ReducerSongToAvgValue;
import proj.mrs.reducers.ReducerTrainTestPartition;
import proj.mrs.reducers.ReducerUserCandidateSongsOne;
import proj.mrs.reducers.ReducerUserCandidateSongsTwo;
import proj.mrs.reducers.ReducerUserSimilarityOne;
import proj.mrs.reducers.ReducerUserSimilarityThree;
import proj.mrs.reducers.ReducerUserSimilarityTwo;
import proj.mrs.reducers.ReducerUserSongs;
import proj.mrs.reducers.ReducerUserSongsWithPlayCount;
import proj.mrs.reducers.ReducerUserToSongsListened;
import proj.mrs.utils.Constants;
import proj.mrs.utils.FloatSumAndCount;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author namanrs
 */
public class AnalysisJob {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //TRAIN-TEST DATASET PARTITION JOB
        boolean runTrainTestPartition = false;
        boolean runOnTrainTest = true;
        //STATISTICS
        boolean runStatsJob = false;

        //USER-SIMILARITY
        boolean runUserSimilarityOne = false;
        boolean runUserSimilarityTwo = false;
        boolean runUserSimilarityThree = false;

        //USER-SONGS_PLAYCOUNT
        boolean runUserSongsPlaycount = false;
        boolean runUserSongs = false;
        boolean runUserSongsWithoutFiltering = false;
        boolean runSongToAvgValue = false;

        //USER-CANDIDATE_SONGS
        boolean runUserCandidateSongsOne = false;
        boolean runUserCandidateSongsTwo = false;

        //RECOMMENDATIONS
        boolean runRecommendationOne = false;
        boolean runRecommendationTwo = false;
        boolean runRecommendationThree = false;

        //COLD_START PROBLEM
        boolean runColdStartOne = true;
        boolean runColdStartTwo = true;
        boolean runColdStartThree = true;
        boolean runColdStartFour = true;

        boolean runColdStartCandidateSongsOne = true;
        boolean runColdStartCandidateSongsTwo = true;

        //EVALUATION
        boolean runEvaluationPhaseOne = true;
        boolean runEvaluationPhaseTwo = true;
        boolean runEvaluationPhaseThree = true;

        boolean runColdStartAnalysis = true;
        boolean runColdStartAnalysisTwo = true;
        //Input path
        String actualInputPath = "";
        String actualOutputPath = "";
        //Output paths
        String outputpathTraining = "";
        String outputpathTesting = "";
        String outputpathStatsJob = "";

        //USER-SIMILARITY
        String outputpathUserSimilarityOne = "";
        String outputpathUserSimilarityTwo = "";
        String outputpathUserSimilarityThree = "";

        String outputpathUserSongsPlaycount = "";
        String outputpathUserSongs = "";
        String outputpathUserSongsWithoutFiltering = "";
        String outputpathSongToAvgValue = "";

        String outputpathUserCandidateSongsOne = "";
        String outputpathUserCandidateSongsTwo = "";

        String outputpathRecommendationOne = "";
        String outputpathRecommendationTwo = "";
        String outputpathRecommendationThree = "";

        //COLD_START PROBLEM
        String outputpathColdStartOne = "";
        String outputpathColdStartTwo = "";
        String outputpathColdStartThree = "";
        String outputpathColdStartFour = "";

        String outputpathColdStartCandidateSongsOne = "";
        String outputpathColdStartCandidateSongsTwo = "";

        String outputpathEvaluationPhaseOne = "";
        String outputpathEvaluationPhaseTwo = "";
        String outputpathEvaluationPhaseThree = "";

        String outputpathColdStartAnalysis = "";
        String outputpathColdStartAnalysisTwo = "";
        //SOUTS
        System.out.println("RUN TRAIN_TEST PARTITION: " + runTrainTestPartition);
        System.out.println("RUN ON TRAIN_TEST: " + runOnTrainTest);
        System.out.println("RUN USER-SIMILARITY - 1: " + runUserSimilarityOne);
        System.out.println("RUN USER-SIMILARITY - 2: " + runUserSimilarityTwo);
        System.out.println("RUN USER-SIMILARITY - 3: " + runUserSimilarityThree);

        System.out.println("RUN USER-SONGS-PLAYCOUNT: " + runUserSongsPlaycount);
        System.out.println("RUN USER-SONGS: " + runUserSongs);
        System.out.println("RUN USER-SONGS WITHOUT FILTERING: " + runUserSongsWithoutFiltering);
        System.out.println("RUN SONG-AVG-VALUE: " + runSongToAvgValue);

        System.out.println("RUN USER-CANDIDATE-SONG - 1: " + runUserCandidateSongsOne);
        System.out.println("RUN USER-CANDIDATE-SONG - 2: " + runUserCandidateSongsTwo);

        System.out.println("RUN RECOMMENDATIONS - 1: " + runRecommendationOne);
        System.out.println("RUN RECOMMENDATIONS - 2: " + runRecommendationTwo);
        System.out.println("RUN RECOMMENDATIONS - 3: " + runRecommendationThree);

        System.out.println("RUN COLD-START - 1: " + runColdStartOne);
        System.out.println("RUN COLD-START - 2: " + runColdStartTwo);
        System.out.println("RUN COLD-START - 3: " + runColdStartThree);
        System.out.println("RUN COLD-START - 4: " + runColdStartFour);

        System.out.println("RUN COLD-START CANDIDATES - 1: " + runColdStartCandidateSongsOne);
        System.out.println("RUN COLD-START CANDIDATES - 2: " + runColdStartCandidateSongsTwo);

        System.out.println("RUN EVALUATION - 1: " + runEvaluationPhaseOne);
        System.out.println("RUN EVALUATION - 2: " + runEvaluationPhaseTwo);
        System.out.println("RUN EVALUATION - 3: " + runEvaluationPhaseThree);
        System.out.println("RUN COLD START ANALYSIS: " + runColdStartAnalysis);
        System.out.println("RUN COLD START ANALYSIS-2: " + runColdStartAnalysisTwo);

        if (runTrainTestPartition) {
            System.out.println("----Train-Test partition----");
            Configuration configurationTrainTest = new Configuration();
            FileSystem fileSystemForTrainTest = FileSystem
                    .get(configurationTrainTest);

            Job trainTestJob = Job.getInstance(configurationTrainTest,
                    "cs555_project_train_test");

            trainTestJob.setJarByClass(AnalysisJob.class);
            trainTestJob.setNumReduceTasks(20);
            trainTestJob.setMapperClass(MapperTrainTestPartition.class);
//            statsJob.setCombinerClass(CombinerUserToSongsListenedStats.class);
            trainTestJob.setReducerClass(ReducerTrainTestPartition.class);
            LazyOutputFormat.setOutputFormatClass(trainTestJob, TextOutputFormat.class);

            trainTestJob.setMapOutputKeyClass(Text.class);
            trainTestJob.setMapOutputValueClass(Text.class);

            trainTestJob.setOutputKeyClass(Text.class);
            trainTestJob.setOutputValueClass(Text.class);
            System.out.println("----Input Train test: " + args[0] + "----");

            FileInputFormat.addInputPath(trainTestJob, new Path(args[0]));

//            outputpathTrainTest = args[1] + "_train_test";
            outputpathTraining = Constants.PATHS.TRAINING_PATH_FOLDER;
            outputpathTesting = Constants.PATHS.TESTING_PATH_FOLDER;

            if (fileSystemForTrainTest.exists(new Path(outputpathTraining))) {
                fileSystemForTrainTest
                        .delete(new Path(outputpathTraining), true);
//                fileSystemForTrainTest.create(new Path(outputpathTraining + Constants.PATHS.TRAINING_PATH_FILES));
            }

            if (fileSystemForTrainTest.exists(new Path(outputpathTesting))) {
                fileSystemForTrainTest
                        .delete(new Path(outputpathTesting), true);
//                fileSystemForTrainTest.create(new Path(outputpathTesting + Constants.PATHS.TESTING_PATH_FILES));
            }

            FileOutputFormat.setOutputPath(trainTestJob, new Path(
                    outputpathTraining));
            trainTestJob.waitForCompletion(true);
            System.out.println("----Output Of Train Test: " + Constants.PATHS.TRAINING_PATH_FOLDER
                    + "----");
        }
        String testingPath = Constants.PATHS.TESTING_PATH_FOLDER + Constants.PATHS.TESTING_PATH_FILES + "-*";
        if (runOnTrainTest) {
            actualInputPath = Constants.PATHS.TRAINING_PATH_FOLDER + Constants.PATHS.TRAINING_PATH_FILES + "-*";
            actualOutputPath = Constants.PATHS.OUTPUT_PATH_FOR_TRAINING_TESTING_PARTITION_RUN;
        } else {
            actualInputPath = args[0];
            actualOutputPath = args[1];
        }

        System.out.println("-----ACTUAL INPUT PATH-----" + actualInputPath);
        System.out.println("-----ACTUAL OUTPUT PATH-----" + actualOutputPath);

        if (runStatsJob) {
            System.out.println("----Stats_Job----");
            Configuration configurationStatsJob = new Configuration();
            FileSystem fileSystemForStatsJob = FileSystem
                    .get(configurationStatsJob);

            Job statsJob = Job.getInstance(configurationStatsJob,
                    "cs555_project_stats_job");

            statsJob.setJarByClass(AnalysisJob.class);
//            statsJob.setInputFormatClass(NLineInputFormat.class);
//            statsJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100000);
            statsJob.setNumReduceTasks(20);
            statsJob.setMapperClass(MapperUserToSongsListenedStats.class);
//            statsJob.setCombinerClass(CombinerUserToSongsListenedStats.class);
            statsJob.setReducerClass(ReducerUserToSongsListened.class);

            statsJob.setMapOutputKeyClass(Text.class);
            statsJob.setMapOutputValueClass(Text.class);

            statsJob.setOutputKeyClass(Text.class);
            statsJob.setOutputValueClass(Text.class);
            System.out.println("----Input stats Job: " + actualInputPath + "----");

            FileInputFormat.addInputPath(statsJob, new Path(actualInputPath));

            outputpathStatsJob = actualOutputPath + "_stats";

            if (fileSystemForStatsJob.exists(new Path(outputpathStatsJob))) {
                fileSystemForStatsJob
                        .delete(new Path(outputpathStatsJob), true);
            }

            FileOutputFormat.setOutputPath(statsJob, new Path(
                    outputpathStatsJob));
            statsJob.waitForCompletion(true);
            System.out.println("----Output Of Stats job: " + outputpathStatsJob
                    + "----");
        } else {
            outputpathStatsJob = actualOutputPath + "_stats";
        }

        if (runSongToAvgValue) {
            System.out.println("----Song To Avg Value----");
            Configuration configurationSongToAvgValue = new Configuration();
            FileSystem fileSystemForSongToAvgValue = FileSystem
                    .get(configurationSongToAvgValue);

            Job SongToAvgValueJob = Job.getInstance(configurationSongToAvgValue,
                    "cs555_project_song_to_avg_value");

            SongToAvgValueJob.setJarByClass(AnalysisJob.class);
            SongToAvgValueJob.setNumReduceTasks(20);
            SongToAvgValueJob.setMapperClass(MapperSongToAvgValue.class);
//            statsJob.setCombinerClass(CombinerUserToSongsListenedStats.class);
            SongToAvgValueJob.setReducerClass(ReducerSongToAvgValue.class);
//            LazyOutputFormat.setOutputFormatClass(trainTestJob, TextOutputFormat.class);

            SongToAvgValueJob.setMapOutputKeyClass(Text.class);
            SongToAvgValueJob.setMapOutputValueClass(IntWritable.class);

            SongToAvgValueJob.setOutputKeyClass(Text.class);
            SongToAvgValueJob.setOutputValueClass(Text.class);
            System.out.println("----Input Song to Avg Value: " + actualInputPath + "----");

            FileInputFormat.addInputPath(SongToAvgValueJob, new Path(args[0]));

            outputpathSongToAvgValue = args[1] + "_song_to_avg_value";

            if (fileSystemForSongToAvgValue.exists(new Path(outputpathSongToAvgValue))) {
                fileSystemForSongToAvgValue
                        .delete(new Path(outputpathSongToAvgValue), true);
            }

            FileOutputFormat.setOutputPath(SongToAvgValueJob, new Path(
                    outputpathSongToAvgValue));
            SongToAvgValueJob.waitForCompletion(true);
            System.out.println("----Output Of Song to Avg Value: " + outputpathSongToAvgValue
                    + "----");
        } else {
            outputpathSongToAvgValue = args[1] + "_song_to_avg_value";
        }

        if (runUserSimilarityOne) {
            System.out.println("----User similarity - 1----");
            Configuration configurationUserSimilarityOne = new Configuration();
            FileSystem fileSystemForUserSimilarityOne = FileSystem
                    .get(configurationUserSimilarityOne);

            Job userSimilarityOneJob = Job.getInstance(configurationUserSimilarityOne,
                    "cs555_user_similarity_one");

            userSimilarityOneJob.setJarByClass(AnalysisJob.class);

            userSimilarityOneJob.setNumReduceTasks(40);
            userSimilarityOneJob.setMapperClass(MapperUserSimilarityOne.class);
            userSimilarityOneJob.setReducerClass(ReducerUserSimilarityOne.class);

            userSimilarityOneJob.setMapOutputKeyClass(Text.class);
            userSimilarityOneJob.setMapOutputValueClass(Text.class);

            userSimilarityOneJob.setOutputKeyClass(Text.class);
            userSimilarityOneJob.setOutputValueClass(Text.class);
            System.out.println("----Input of User similarity - 1 Job: " + actualInputPath + "----");

            FileInputFormat.addInputPath(userSimilarityOneJob, new Path(actualInputPath));

            outputpathUserSimilarityOne = actualOutputPath + "_user_similarity_one";

            if (fileSystemForUserSimilarityOne.exists(new Path(outputpathUserSimilarityOne))) {
                fileSystemForUserSimilarityOne
                        .delete(new Path(outputpathUserSimilarityOne), true);
            }

            FileOutputFormat.setOutputPath(userSimilarityOneJob, new Path(
                    outputpathUserSimilarityOne));
            userSimilarityOneJob.waitForCompletion(true);
            System.out.println("----Output Of User similarity - 1 job: " + outputpathUserSimilarityOne
                    + "----");
        } else {
            outputpathUserSimilarityOne = actualOutputPath + "_user_similarity_one";
//            outputpathUserSimilarityOne = "/cs555/Term_Project/Output/Training/Output" + "_user_similarity_one";
        }

        if (runUserSimilarityTwo) {
            System.out.println("----User similarity - 2----");
            Configuration configurationUserSimilarityTwo = new Configuration();
            FileSystem fileSystemForUserSimilarityTwo = FileSystem
                    .get(configurationUserSimilarityTwo);

            Job userSimilarityTwoJob = Job.getInstance(configurationUserSimilarityTwo,
                    "cs555_user_similarity_two");

            userSimilarityTwoJob.setJarByClass(AnalysisJob.class);
            userSimilarityTwoJob.setInputFormatClass(NLineInputFormat.class);
            userSimilarityTwoJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            userSimilarityTwoJob.setNumReduceTasks(320);
            userSimilarityTwoJob.setMapperClass(MapperUserSimilarityTwo.class);
            userSimilarityTwoJob.setCombinerClass(CombinerUserSimilarityTwo.class);
            userSimilarityTwoJob.setReducerClass(ReducerUserSimilarityTwo.class);

            userSimilarityTwoJob.setMapOutputKeyClass(Text.class);
            userSimilarityTwoJob.setMapOutputValueClass(FloatWritable.class);

            userSimilarityTwoJob.setOutputKeyClass(Text.class);
            userSimilarityTwoJob.setOutputValueClass(Text.class);
            System.out.println("----Input of User similarity - 2 Job: " + outputpathUserSimilarityOne + "----");

            FileInputFormat.addInputPath(userSimilarityTwoJob, new Path(outputpathUserSimilarityOne));

            outputpathUserSimilarityTwo = actualOutputPath + "_user_similarity_two";

            if (fileSystemForUserSimilarityTwo.exists(new Path(outputpathUserSimilarityTwo))) {
                fileSystemForUserSimilarityTwo
                        .delete(new Path(outputpathUserSimilarityTwo), true);
            }

            FileOutputFormat.setOutputPath(userSimilarityTwoJob, new Path(
                    outputpathUserSimilarityTwo));
            userSimilarityTwoJob.waitForCompletion(true);
            System.out.println("----Output Of User similarity - 2 job: " + outputpathUserSimilarityTwo
                    + "----");
        } else {
            outputpathUserSimilarityTwo = actualOutputPath + "_user_similarity_two";
//            outputpathUserSimilarityTwo = "/cs555/Term_Project/Output/Training/Output" + "_user_similarity_two";
        }
        if (runUserSimilarityThree) {
            System.out.println("----User similarity - 3----");
            Configuration configurationUserSimilarityThree = new Configuration();
            FileSystem fileSystemForUserSimilarityThree = FileSystem
                    .get(configurationUserSimilarityThree);

            Job userSimilarityThreeJob = Job.getInstance(configurationUserSimilarityThree,
                    "cs555_user_similarity_three");

            userSimilarityThreeJob.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            userSimilarityThreeJob.setNumReduceTasks(320);
            userSimilarityThreeJob.setMapperClass(MapperUserSimilarityThree.class);
            userSimilarityThreeJob.setReducerClass(ReducerUserSimilarityThree.class);

            userSimilarityThreeJob.setMapOutputKeyClass(Text.class);
            userSimilarityThreeJob.setMapOutputValueClass(Text.class);

            userSimilarityThreeJob.setOutputKeyClass(Text.class);
            userSimilarityThreeJob.setOutputValueClass(Text.class);
            System.out.println("----Input of User similarity - 3 Job: " + outputpathUserSimilarityTwo + "----");

            FileInputFormat.addInputPath(userSimilarityThreeJob, new Path(outputpathUserSimilarityTwo));

            outputpathUserSimilarityThree = actualOutputPath + "_user_similarity_three";

            if (fileSystemForUserSimilarityThree.exists(new Path(outputpathUserSimilarityThree))) {
                fileSystemForUserSimilarityThree
                        .delete(new Path(outputpathUserSimilarityThree), true);
            }

            FileOutputFormat.setOutputPath(userSimilarityThreeJob, new Path(
                    outputpathUserSimilarityThree));
            userSimilarityThreeJob.waitForCompletion(true);
            System.out.println("----Output Of User similarity - 3 job: " + outputpathUserSimilarityThree
                    + "----");
        } else {
            outputpathUserSimilarityThree = actualOutputPath + "_user_similarity_three";
//            outputpathUserSimilarityThree = "/cs555/Term_Project/Output/Training/Output" + "_user_similarity_three";
        }
        if (runUserSongs) {
            System.out.println("----USER-SONGS----");
            Configuration configurationUserSongs = new Configuration();
            FileSystem fileSystemForUserSongs = FileSystem
                    .get(configurationUserSongs);

            Job userSongs = Job.getInstance(configurationUserSongs,
                    "cs555_user_song");

            userSongs.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            userSongs.setNumReduceTasks(20);
            userSongs.setMapperClass(MapperUserSongs.class);
            userSongs.setReducerClass(ReducerUserSongs.class);

            userSongs.setMapOutputKeyClass(Text.class);
            userSongs.setMapOutputValueClass(Text.class);

            userSongs.setOutputKeyClass(Text.class);
            userSongs.setOutputValueClass(Text.class);
            System.out.println("----Input of USER-SONGS Job: " + actualInputPath + "----");

            FileInputFormat.addInputPath(userSongs, new Path(actualInputPath));

            outputpathUserSongs = actualOutputPath + "_user_songs";

            if (fileSystemForUserSongs.exists(new Path(outputpathUserSongs))) {
                fileSystemForUserSongs
                        .delete(new Path(outputpathUserSongs), true);
            }

            FileOutputFormat.setOutputPath(userSongs, new Path(
                    outputpathUserSongs));
            userSongs.waitForCompletion(true);
            System.out.println("----Output Of USER-SONGS  job: " + outputpathUserSongs
                    + "----");
        } else {
            outputpathUserSongs = actualOutputPath + "_user_songs";
        }

        if (runUserSongsWithoutFiltering) {
            System.out.println("----USER-SONGS WITHOUT FILTERING----");
            Configuration configurationUserSongsWithoutFiltering = new Configuration();
            FileSystem fileSystemForUserSongsWithoutFiltering = FileSystem
                    .get(configurationUserSongsWithoutFiltering);

            Job userSongsWithoutFiltering = Job.getInstance(configurationUserSongsWithoutFiltering,
                    "cs555_user_song_without_filtering");

            userSongsWithoutFiltering.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            userSongsWithoutFiltering.setNumReduceTasks(20);
            userSongsWithoutFiltering.setMapperClass(MapperUserSongsWithoutFiltering.class);
            userSongsWithoutFiltering.setReducerClass(ReducerUserSongs.class);

            userSongsWithoutFiltering.setMapOutputKeyClass(Text.class);
            userSongsWithoutFiltering.setMapOutputValueClass(Text.class);

            userSongsWithoutFiltering.setOutputKeyClass(Text.class);
            userSongsWithoutFiltering.setOutputValueClass(Text.class);
            System.out.println("----Input of USER-SONGS WITHOUT FILTERING Job: " + actualInputPath + "----");

            FileInputFormat.addInputPath(userSongsWithoutFiltering, new Path(actualInputPath));

            outputpathUserSongsWithoutFiltering = actualOutputPath + "_user_songs_without_filtering";

            if (fileSystemForUserSongsWithoutFiltering.exists(new Path(outputpathUserSongsWithoutFiltering))) {
                fileSystemForUserSongsWithoutFiltering
                        .delete(new Path(outputpathUserSongsWithoutFiltering), true);
            }

            FileOutputFormat.setOutputPath(userSongsWithoutFiltering, new Path(
                    outputpathUserSongsWithoutFiltering));
            userSongsWithoutFiltering.waitForCompletion(true);
            System.out.println("----Output Of USER-SONGS WITHOUT FILTERING job: " + outputpathUserSongsWithoutFiltering
                    + "----");
        } else {
            outputpathUserSongsWithoutFiltering = actualOutputPath + "_user_songs_without_filtering";
        }

        if (runUserSongsPlaycount) {
            System.out.println("----USER-SONGS-PLAYCOUNT----");
            Configuration configurationUserSongPlayCount = new Configuration();
            FileSystem fileSystemForUserSongPlayCount = FileSystem
                    .get(configurationUserSongPlayCount);

            Job userSongPlayCount = Job.getInstance(configurationUserSongPlayCount,
                    "cs555_user_song_playcount");

            userSongPlayCount.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            userSongPlayCount.setNumReduceTasks(20);
            userSongPlayCount.setMapperClass(MapperUserSongsWithPlayCount.class);
            userSongPlayCount.setReducerClass(ReducerUserSongsWithPlayCount.class);

            userSongPlayCount.setMapOutputKeyClass(Text.class);
            userSongPlayCount.setMapOutputValueClass(Text.class);

            userSongPlayCount.setOutputKeyClass(Text.class);
            userSongPlayCount.setOutputValueClass(Text.class);
            System.out.println("----Input of USER-SONGS-PLAYCOUNT Job: " + actualInputPath + "----");

            FileInputFormat.addInputPath(userSongPlayCount, new Path(actualInputPath));

            outputpathUserSongsPlaycount = actualOutputPath + "_user_song_playcount";

            if (fileSystemForUserSongPlayCount.exists(new Path(outputpathUserSongsPlaycount))) {
                fileSystemForUserSongPlayCount
                        .delete(new Path(outputpathUserSongsPlaycount), true);
            }

            FileOutputFormat.setOutputPath(userSongPlayCount, new Path(
                    outputpathUserSongsPlaycount));
            userSongPlayCount.waitForCompletion(true);
            System.out.println("----Output Of USER-SONGS-PLAYCOUNT - 3 job: " + outputpathUserSongsPlaycount
                    + "----");
        } else {
            outputpathUserSongsPlaycount = actualOutputPath + "_user_song_playcount";
        }

        if (runUserCandidateSongsOne) {
            System.out.println("----USER-CANDIDATE-SONGS-1----");
            Configuration configurationUserCandidateSongsOne = new Configuration();
            FileSystem fileSystemForUserCandidateSongsOne = FileSystem
                    .get(configurationUserCandidateSongsOne);

            Job userCandidateSongsOne = Job.getInstance(configurationUserCandidateSongsOne,
                    "cs555_user_candidate_songs_one");

            userCandidateSongsOne.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            userCandidateSongsOne.setNumReduceTasks(40);
            userCandidateSongsOne.setMapperClass(MapperUserCandidateSongsOne.class);
            userCandidateSongsOne.setReducerClass(ReducerUserCandidateSongsOne.class);

            userCandidateSongsOne.setMapOutputKeyClass(Text.class);
            userCandidateSongsOne.setMapOutputValueClass(Text.class);

            userCandidateSongsOne.setOutputKeyClass(Text.class);
            userCandidateSongsOne.setOutputValueClass(Text.class);
            System.out.println("----Input of USER-CANDIDATE-SONGS-1 Job: " + outputpathUserSongs + "----");
            System.out.println("----Input of USER-CANDIDATE-SONGS-1 Job: " + outputpathUserSimilarityThree + "----");

            FileInputFormat.addInputPath(userCandidateSongsOne, new Path(outputpathUserSongs));
            FileInputFormat.addInputPath(userCandidateSongsOne, new Path(outputpathUserSimilarityThree));

            outputpathUserCandidateSongsOne = actualOutputPath + "_user_candidate_songs_one";

            if (fileSystemForUserCandidateSongsOne.exists(new Path(outputpathUserCandidateSongsOne))) {
                fileSystemForUserCandidateSongsOne
                        .delete(new Path(outputpathUserCandidateSongsOne), true);
            }

            FileOutputFormat.setOutputPath(userCandidateSongsOne, new Path(
                    outputpathUserCandidateSongsOne));
            userCandidateSongsOne.waitForCompletion(true);
            System.out.println("----Output Of USER-CANDIDATE-SONGS-1 job: " + outputpathUserCandidateSongsOne
                    + "----");
        } else {
            outputpathUserCandidateSongsOne = actualOutputPath + "_user_candidate_songs_one";
        }

        if (runUserCandidateSongsTwo) {
            System.out.println("----USER-CANDIDATE-SONGS-2----");
            Configuration configurationUserCandidateSongsTwo = new Configuration();
            FileSystem fileSystemForUserCandidateSongsTwo = FileSystem
                    .get(configurationUserCandidateSongsTwo);

            Job userCandidateSongsTwo = Job.getInstance(configurationUserCandidateSongsTwo,
                    "cs555_user_candidate_songs_two");

            userCandidateSongsTwo.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            userCandidateSongsTwo.setNumReduceTasks(320);
            userCandidateSongsTwo.setMapperClass(MapperUserCandidateSongsTwo.class);
            userCandidateSongsTwo.setReducerClass(ReducerUserCandidateSongsTwo.class);

            userCandidateSongsTwo.setMapOutputKeyClass(Text.class);
            userCandidateSongsTwo.setMapOutputValueClass(Text.class);

            userCandidateSongsTwo.setOutputKeyClass(Text.class);
            userCandidateSongsTwo.setOutputValueClass(Text.class);
            System.out.println("----Input of USER-CANDIDATE-SONGS-2 Job: " + outputpathUserSongs + "----");
            System.out.println("----Input of USER-CANDIDATE-SONGS-2 Job: " + outputpathUserCandidateSongsOne + "----");

            FileInputFormat.addInputPath(userCandidateSongsTwo, new Path(outputpathUserSongs));
            FileInputFormat.addInputPath(userCandidateSongsTwo, new Path(outputpathUserCandidateSongsOne));

            outputpathUserCandidateSongsTwo = actualOutputPath + "_user_candidate_songs_two";

            if (fileSystemForUserCandidateSongsTwo.exists(new Path(outputpathUserCandidateSongsTwo))) {
                fileSystemForUserCandidateSongsTwo
                        .delete(new Path(outputpathUserCandidateSongsTwo), true);
            }

            FileOutputFormat.setOutputPath(userCandidateSongsTwo, new Path(
                    outputpathUserCandidateSongsTwo));
            userCandidateSongsTwo.waitForCompletion(true);
            System.out.println("----Output Of USER-CANDIDATE-SONGS-2 job: " + outputpathUserCandidateSongsTwo
                    + "----");
        } else {
            outputpathUserCandidateSongsTwo = actualOutputPath + "_user_candidate_songs_two";
        }

        if (runRecommendationOne) {
            System.out.println("----RECOMMENDATIONS-1----");
            Configuration configurationRecommendationsOne = new Configuration();
            FileSystem fileSystemForRecommendationsOne = FileSystem
                    .get(configurationRecommendationsOne);

            Job RecommendationsOne = Job.getInstance(configurationRecommendationsOne,
                    "cs555_recommendations_one");

            RecommendationsOne.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            RecommendationsOne.setNumReduceTasks(320);
            RecommendationsOne.setMapperClass(MapperRecommendationOne.class);
            RecommendationsOne.setReducerClass(ReducerRecommendationOne.class);

            RecommendationsOne.setMapOutputKeyClass(Text.class);
            RecommendationsOne.setMapOutputValueClass(Text.class);

            RecommendationsOne.setOutputKeyClass(Text.class);
            RecommendationsOne.setOutputValueClass(Text.class);
            System.out.println("----Input of Recommendations Job: " + outputpathUserSongsPlaycount + "----");
            System.out.println("----Input of Recommendations Job: " + outputpathUserSimilarityThree + "----");
            System.out.println("----Input of Recommendations Job: " + outputpathUserCandidateSongsTwo + "----");

            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSongsPlaycount));
            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathRecommendationOne = actualOutputPath + "_user_recommendations_one";

            if (fileSystemForRecommendationsOne.exists(new Path(outputpathRecommendationOne))) {
                fileSystemForRecommendationsOne
                        .delete(new Path(outputpathRecommendationOne), true);
            }

            FileOutputFormat.setOutputPath(RecommendationsOne, new Path(
                    outputpathRecommendationOne));
            RecommendationsOne.waitForCompletion(true);
            System.out.println("----Output Of RECOMMENDATIONS-1 job: " + outputpathRecommendationOne
                    + "----");
        } else {
            outputpathRecommendationOne = actualOutputPath + "_user_recommendations_one";
        }

        if (runRecommendationTwo) {
            System.out.println("----RECOMMENDATIONS-2----");
            Configuration configurationRecommendationsTwo = new Configuration();
            FileSystem fileSystemForRecommendaitonsTwo = FileSystem
                    .get(configurationRecommendationsTwo);

            Job RecommendationsTwo = Job.getInstance(configurationRecommendationsTwo,
                    "cs555_recommendations_two");

            RecommendationsTwo.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            RecommendationsTwo.setNumReduceTasks(320);
            RecommendationsTwo.setMapperClass(MapperRecommendationTwo.class);
            RecommendationsTwo.setReducerClass(ReducerRecommendationTwo.class);

            RecommendationsTwo.setMapOutputKeyClass(Text.class);
            RecommendationsTwo.setMapOutputValueClass(Text.class);

            RecommendationsTwo.setOutputKeyClass(Text.class);
            RecommendationsTwo.setOutputValueClass(Text.class);
            System.out.println("----Input of Recommendations Job: " + outputpathRecommendationOne + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserSimilarityThree + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserCandidateSongsTwo + "----");

            FileInputFormat.addInputPath(RecommendationsTwo, new Path(outputpathRecommendationOne));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathRecommendationTwo = actualOutputPath + "_user_recommendations_two";

            if (fileSystemForRecommendaitonsTwo.exists(new Path(outputpathRecommendationTwo))) {
                fileSystemForRecommendaitonsTwo
                        .delete(new Path(outputpathRecommendationTwo), true);
            }

            FileOutputFormat.setOutputPath(RecommendationsTwo, new Path(
                    outputpathRecommendationTwo));
            RecommendationsTwo.waitForCompletion(true);
            System.out.println("----Output Of RECOMMENDATIONS-2 job: " + outputpathRecommendationTwo
                    + "----");
        } else {
            outputpathRecommendationTwo = actualOutputPath + "_user_recommendations_two";
        }

        if (runRecommendationThree) {
            System.out.println("----RECOMMENDATIONS-3----");
            Configuration configurationRecommendationsThree = new Configuration();
            FileSystem fileSystemForRecommendaitonsThree = FileSystem
                    .get(configurationRecommendationsThree);

            Job RecommendationsThree = Job.getInstance(configurationRecommendationsThree,
                    "cs555_recommendations_three");

            RecommendationsThree.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            RecommendationsThree.setNumReduceTasks(320);
            RecommendationsThree.setMapperClass(MapperRecommendationThree.class);
            RecommendationsThree.setReducerClass(ReducerRecommendationThree.class);

            RecommendationsThree.setMapOutputKeyClass(Text.class);
            RecommendationsThree.setMapOutputValueClass(Text.class);

            RecommendationsThree.setOutputKeyClass(Text.class);
            RecommendationsThree.setOutputValueClass(Text.class);
            System.out.println("----Input of Recommendations-3Job: " + outputpathRecommendationTwo + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserSimilarityThree + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserCandidateSongsTwo + "----");

            FileInputFormat.addInputPath(RecommendationsThree, new Path(outputpathRecommendationTwo));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathRecommendationThree = actualOutputPath + "_user_recommendations_three";

            if (fileSystemForRecommendaitonsThree.exists(new Path(outputpathRecommendationThree))) {
                fileSystemForRecommendaitonsThree
                        .delete(new Path(outputpathRecommendationThree), true);
            }

            FileOutputFormat.setOutputPath(RecommendationsThree, new Path(
                    outputpathRecommendationThree));
            RecommendationsThree.waitForCompletion(true);
            System.out.println("----Output Of RECOMMENDATIONS-3 job: " + outputpathRecommendationThree
                    + "----");
        } else {
            outputpathRecommendationThree = actualOutputPath + "_user_recommendations_three";
        }

        if (runColdStartOne) {
            System.out.println("----COLD START-1----");
            Configuration configurationColdStartOne = new Configuration();
            FileSystem fileSystemForColdStartOne = FileSystem
                    .get(configurationColdStartOne);

            Job ColdStartOne = Job.getInstance(configurationColdStartOne,
                    "cs555_coldstart_one");

            ColdStartOne.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            ColdStartOne.setNumReduceTasks(20);
            ColdStartOne.setMapperClass(MapperColdStartOne.class);
            ColdStartOne.setReducerClass(ReducerColdStartOne.class);

            ColdStartOne.setMapOutputKeyClass(Text.class);
            ColdStartOne.setMapOutputValueClass(Text.class);

            ColdStartOne.setOutputKeyClass(Text.class);
            ColdStartOne.setOutputValueClass(Text.class);
            System.out.println("----Input of Cold Start-1 Job: " + actualInputPath + "----");
            System.out.println("----Input of Cold Start-1 Job: " + outputpathRecommendationThree + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserSimilarityThree + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserCandidateSongsTwo + "----");

            FileInputFormat.addInputPath(ColdStartOne, new Path(actualInputPath));
            FileInputFormat.addInputPath(ColdStartOne, new Path(outputpathRecommendationThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathColdStartOne = actualOutputPath + "_coldstart_one";

            if (fileSystemForColdStartOne.exists(new Path(outputpathColdStartOne))) {
                fileSystemForColdStartOne
                        .delete(new Path(outputpathColdStartOne), true);
            }

            FileOutputFormat.setOutputPath(ColdStartOne, new Path(
                    outputpathColdStartOne));
            ColdStartOne.waitForCompletion(true);
            System.out.println("----Output Of COLD-START-1 job: " + outputpathColdStartOne
                    + "----");
        } else {
            outputpathColdStartOne = actualOutputPath + "_coldstart_one";
        }

        if (runColdStartTwo) {
            System.out.println("----COLD START-2----");
            Configuration configurationColdStartTwo = new Configuration();
            FileSystem fileSystemForColdStartTwo = FileSystem
                    .get(configurationColdStartTwo);

            Job ColdStartTwo = Job.getInstance(configurationColdStartTwo,
                    "cs555_coldstart_two");

            ColdStartTwo.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            ColdStartTwo.setNumReduceTasks(40);
            ColdStartTwo.setMapperClass(MapperColdStartTwo.class);
            ColdStartTwo.setReducerClass(ReducerColdStartTwo.class);

            ColdStartTwo.setMapOutputKeyClass(Text.class);
            ColdStartTwo.setMapOutputValueClass(Text.class);

            ColdStartTwo.setOutputKeyClass(Text.class);
            ColdStartTwo.setOutputValueClass(Text.class);
            System.out.println("----Input of Cold Start-2 Job: " + outputpathColdStartOne + "----");
            System.out.println("----Input of Cold Start-2 Job: " + outputpathUserSimilarityOne + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserSimilarityThree + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserCandidateSongsTwo + "----");

            FileInputFormat.addInputPath(ColdStartTwo, new Path(outputpathColdStartOne));
            FileInputFormat.addInputPath(ColdStartTwo, new Path(outputpathUserSimilarityOne));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathColdStartTwo = actualOutputPath + "_coldstart_two";

            if (fileSystemForColdStartTwo.exists(new Path(outputpathColdStartTwo))) {
                fileSystemForColdStartTwo
                        .delete(new Path(outputpathColdStartTwo), true);
            }

            FileOutputFormat.setOutputPath(ColdStartTwo, new Path(
                    outputpathColdStartTwo));
            ColdStartTwo.waitForCompletion(true);
            System.out.println("----Output Of COLD-START-2 job: " + outputpathColdStartTwo
                    + "----");
        } else {
            outputpathColdStartTwo = actualOutputPath + "_coldstart_two";
        }

        if (runColdStartThree) {
            System.out.println("----COLD START-3----");
            Configuration configurationColdStartThree = new Configuration();
            FileSystem fileSystemForColdStartThree = FileSystem
                    .get(configurationColdStartThree);

            Job ColdStartThree = Job.getInstance(configurationColdStartThree,
                    "cs555_coldstart_three");

            ColdStartThree.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            ColdStartThree.setNumReduceTasks(80);
            ColdStartThree.setMapperClass(MapperColdStartThree.class);
            ColdStartThree.setReducerClass(ReducerColdStartThree.class);

            ColdStartThree.setMapOutputKeyClass(Text.class);
            ColdStartThree.setMapOutputValueClass(Text.class);

            ColdStartThree.setOutputKeyClass(Text.class);
            ColdStartThree.setOutputValueClass(Text.class);
            System.out.println("----Input of Cold Start-3 Job: " + outputpathColdStartTwo + "----");
//            System.out.println("----Input of Cold Start-3 Job: " + outputpathUserSimilarityOne + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserSimilarityThree + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserCandidateSongsTwo + "----");

            FileInputFormat.addInputPath(ColdStartThree, new Path(outputpathColdStartTwo));
//            FileInputFormat.addInputPath(ColdStartThree, new Path(outputpathUserSimilarityOne));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathColdStartThree = actualOutputPath + "_coldstart_three";

            if (fileSystemForColdStartThree.exists(new Path(outputpathColdStartThree))) {
                fileSystemForColdStartThree
                        .delete(new Path(outputpathColdStartThree), true);
            }

            FileOutputFormat.setOutputPath(ColdStartThree, new Path(
                    outputpathColdStartThree));
            ColdStartThree.waitForCompletion(true);
            System.out.println("----Output Of COLD-START-3 job: " + outputpathColdStartThree
                    + "----");
        } else {
            outputpathColdStartThree = actualOutputPath + "_coldstart_three";
        }

        if (runColdStartFour) {
            System.out.println("----COLD START-4----");
            Configuration configurationColdStartFour = new Configuration();
            FileSystem fileSystemForColdStartFour = FileSystem
                    .get(configurationColdStartFour);

            Job ColdStartFour = Job.getInstance(configurationColdStartFour,
                    "cs555_coldstart_four");

            ColdStartFour.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            ColdStartFour.setNumReduceTasks(40);
            ColdStartFour.setMapperClass(MapperColdStartFour.class);
            ColdStartFour.setReducerClass(ReducerColdStartFour.class);

            ColdStartFour.setMapOutputKeyClass(Text.class);
            ColdStartFour.setMapOutputValueClass(Text.class);

            ColdStartFour.setOutputKeyClass(Text.class);
            ColdStartFour.setOutputValueClass(Text.class);
            System.out.println("----Input of Cold Start-4 Job: " + outputpathColdStartThree + "----");
//            System.out.println("----Input of Cold Start-3 Job: " + outputpathUserSimilarityOne + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserSimilarityThree + "----");
//            System.out.println("----Input of Recommendations Job: " + outputpathUserCandidateSongsTwo + "----");

            FileInputFormat.addInputPath(ColdStartFour, new Path(outputpathColdStartThree));
//            FileInputFormat.addInputPath(ColdStartThree, new Path(outputpathUserSimilarityOne));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathColdStartFour = actualOutputPath + "_coldstart_four";

            if (fileSystemForColdStartFour.exists(new Path(outputpathColdStartFour))) {
                fileSystemForColdStartFour
                        .delete(new Path(outputpathColdStartFour), true);
            }

            FileOutputFormat.setOutputPath(ColdStartFour, new Path(
                    outputpathColdStartFour));
            ColdStartFour.waitForCompletion(true);
            System.out.println("----Output Of COLD-START-4 job: " + outputpathColdStartFour
                    + "----");
        } else {
            outputpathColdStartFour = actualOutputPath + "_coldstart_four";
        }

        if (runColdStartCandidateSongsOne) {
            System.out.println("----COLD START CANDIDATE-1----");
            Configuration configurationColdStartCandidateOne = new Configuration();
            FileSystem fileSystemForColdStartCandidateOne = FileSystem
                    .get(configurationColdStartCandidateOne);

            Job ColdStartCandidateOne = Job.getInstance(configurationColdStartCandidateOne,
                    "cs555_coldstart_candidate_one");

            ColdStartCandidateOne.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            ColdStartCandidateOne.setNumReduceTasks(40);
            ColdStartCandidateOne.setMapperClass(MapperUserCandidateSongsOne.class);
            ColdStartCandidateOne.setReducerClass(ReducerUserCandidateSongsOne.class);

            ColdStartCandidateOne.setMapOutputKeyClass(Text.class);
            ColdStartCandidateOne.setMapOutputValueClass(Text.class);

            ColdStartCandidateOne.setOutputKeyClass(Text.class);
            ColdStartCandidateOne.setOutputValueClass(Text.class);
            System.out.println("----Input of Cold Start Candidate-1 Job: " + outputpathColdStartFour + "----");
            System.out.println("----Input of Cold Start Candidate-1 Job: " + outputpathUserSongsWithoutFiltering + "----");

            FileInputFormat.addInputPath(ColdStartCandidateOne, new Path(outputpathColdStartFour));
            FileInputFormat.addInputPath(ColdStartCandidateOne, new Path(outputpathUserSongsWithoutFiltering));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathColdStartCandidateSongsOne = actualOutputPath + "_coldstart_candidate_one";

            if (fileSystemForColdStartCandidateOne.exists(new Path(outputpathColdStartCandidateSongsOne))) {
                fileSystemForColdStartCandidateOne
                        .delete(new Path(outputpathColdStartCandidateSongsOne), true);
            }

            FileOutputFormat.setOutputPath(ColdStartCandidateOne, new Path(
                    outputpathColdStartCandidateSongsOne));
            ColdStartCandidateOne.waitForCompletion(true);
            System.out.println("----Output Of COLD-START-CANDIDATE-1 job: " + outputpathColdStartCandidateSongsOne
                    + "----");
        } else {
            outputpathColdStartCandidateSongsOne = actualOutputPath + "_coldstart_candidate_one";
        }

        if (runColdStartCandidateSongsTwo) {
            System.out.println("----COLD START CANDIDATE-2----");
            Configuration configurationColdStartCandidateTwo = new Configuration();
            FileSystem fileSystemForColdStartCandidateTwo = FileSystem
                    .get(configurationColdStartCandidateTwo);

            Job ColdStartCandidateTwo = Job.getInstance(configurationColdStartCandidateTwo,
                    "cs555_coldstart_candidate_two");

            ColdStartCandidateTwo.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            ColdStartCandidateTwo.setNumReduceTasks(40);
            ColdStartCandidateTwo.setMapperClass(MapperUserCandidateSongsTwo.class);
            ColdStartCandidateTwo.setReducerClass(ReducerColdStartCandidateSongsTwo.class);

            ColdStartCandidateTwo.setMapOutputKeyClass(Text.class);
            ColdStartCandidateTwo.setMapOutputValueClass(Text.class);

            ColdStartCandidateTwo.setOutputKeyClass(Text.class);
            ColdStartCandidateTwo.setOutputValueClass(Text.class);
            System.out.println("----Input of Cold Start Candidate-2 Job: " + outputpathColdStartCandidateSongsOne + "----");
            System.out.println("----Input of Cold Start Candidate-2 Job: " + outputpathUserSongsWithoutFiltering + "----");
//            System.out.println("----Input of Cold Start-3 Job: " + outputpathUserSongs + "----");

            FileInputFormat.addInputPath(ColdStartCandidateTwo, new Path(outputpathColdStartCandidateSongsOne));
            FileInputFormat.addInputPath(ColdStartCandidateTwo, new Path(outputpathUserSongsWithoutFiltering));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathColdStartCandidateSongsTwo = actualOutputPath + "_coldstart_candidate_two";

            if (fileSystemForColdStartCandidateTwo.exists(new Path(outputpathColdStartCandidateSongsTwo))) {
                fileSystemForColdStartCandidateTwo
                        .delete(new Path(outputpathColdStartCandidateSongsTwo), true);
            }

            FileOutputFormat.setOutputPath(ColdStartCandidateTwo, new Path(
                    outputpathColdStartCandidateSongsTwo));
            ColdStartCandidateTwo.waitForCompletion(true);
            System.out.println("----Output Of COLD-START-CANDIDATE-2 job: " + outputpathColdStartCandidateSongsTwo
                    + "----");
        } else {
            outputpathColdStartCandidateSongsTwo = actualOutputPath + "_coldstart_candidate_two";
        }

        if (runEvaluationPhaseOne) {
            System.out.println("----EVALUATION-1----");
            Configuration configurationEvaluationPhaseOne = new Configuration();
            FileSystem fileSystemForEvaluationPhaseOne = FileSystem
                    .get(configurationEvaluationPhaseOne);

            Job EvaluationPhaseOne = Job.getInstance(configurationEvaluationPhaseOne,
                    "cs555_evaluation_phase_one");

            EvaluationPhaseOne.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            EvaluationPhaseOne.setNumReduceTasks(20);
            EvaluationPhaseOne.setMapperClass(MapperEvaluationPhaseOne.class);
            EvaluationPhaseOne.setReducerClass(ReducerEvaluationPhaseOne.class);

            EvaluationPhaseOne.setMapOutputKeyClass(Text.class);
            EvaluationPhaseOne.setMapOutputValueClass(Text.class);

            EvaluationPhaseOne.setOutputKeyClass(Text.class);
            EvaluationPhaseOne.setOutputValueClass(Text.class);

            System.out.println("----Input of Evaluation-1 Job: " + outputpathColdStartCandidateSongsTwo + "----");
            System.out.println("----Input of Evaluation-1 Job: " + outputpathRecommendationThree + "----");
//            System.out.println("----Input of Cold Start-3 Job: " + outputpathUserSongs + "----");

            FileInputFormat.addInputPath(EvaluationPhaseOne, new Path(outputpathColdStartCandidateSongsTwo));
            FileInputFormat.addInputPath(EvaluationPhaseOne, new Path(outputpathRecommendationThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathEvaluationPhaseOne = actualOutputPath + "_evaluation_one";

            if (fileSystemForEvaluationPhaseOne
                    .exists(new Path(outputpathEvaluationPhaseOne))) {
                fileSystemForEvaluationPhaseOne
                        .delete(new Path(outputpathEvaluationPhaseOne), true);
            }

            FileOutputFormat.setOutputPath(EvaluationPhaseOne, new Path(
                    outputpathEvaluationPhaseOne));
            EvaluationPhaseOne.waitForCompletion(true);
            System.out.println("----Output Of EVALUATION-1 job: " + outputpathEvaluationPhaseOne
                    + "----");
        } else {
            outputpathEvaluationPhaseOne = actualOutputPath + "_evaluation_one";
        }

        if (runEvaluationPhaseTwo) {
            System.out.println("----EVALUATION-2----");
            Configuration configurationEvaluationPhaseTwo = new Configuration();
            FileSystem fileSystemForEvaluationPhaseTwo = FileSystem
                    .get(configurationEvaluationPhaseTwo);

            Job EvaluationPhaseTwo = Job.getInstance(configurationEvaluationPhaseTwo,
                    "cs555_evaluation_phase_two");

            EvaluationPhaseTwo.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            EvaluationPhaseTwo.setNumReduceTasks(20);
            EvaluationPhaseTwo.setMapperClass(MapperEvaluationPhaseTwo.class);
            EvaluationPhaseTwo.setReducerClass(ReducerEvaluationPhaseTwo.class);

            EvaluationPhaseTwo.setMapOutputKeyClass(Text.class);
            EvaluationPhaseTwo.setMapOutputValueClass(Text.class);

            EvaluationPhaseTwo.setOutputKeyClass(Text.class);
            EvaluationPhaseTwo.setOutputValueClass(Text.class);

            System.out.println("----Input of Evaluation-2 Job: " + outputpathEvaluationPhaseOne + "----");
            System.out.println("----Input of Evaluation-2 Job: " + testingPath + "----");
//            System.out.println("----Input of Cold Start-3 Job: " + outputpathUserSongs + "----");

            FileInputFormat.addInputPath(EvaluationPhaseTwo, new Path(outputpathEvaluationPhaseOne));
            FileInputFormat.addInputPath(EvaluationPhaseTwo, new Path(testingPath));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathEvaluationPhaseTwo = actualOutputPath + "_evaluation_two";

            if (fileSystemForEvaluationPhaseTwo
                    .exists(new Path(outputpathEvaluationPhaseTwo))) {
                fileSystemForEvaluationPhaseTwo
                        .delete(new Path(outputpathEvaluationPhaseTwo), true);
            }

            FileOutputFormat.setOutputPath(EvaluationPhaseTwo, new Path(
                    outputpathEvaluationPhaseTwo));
            EvaluationPhaseTwo.waitForCompletion(true);
            System.out.println("----Output Of EVALUATION-2 job: " + outputpathEvaluationPhaseTwo
                    + "----");
        } else {
            outputpathEvaluationPhaseTwo = actualOutputPath + "_evaluation_two";
        }

        if (runEvaluationPhaseThree) {
            System.out.println("----EVALUATION-3----");
            Configuration configurationEvaluationPhaseThree = new Configuration();
            FileSystem fileSystemForEvaluationPhaseThree = FileSystem
                    .get(configurationEvaluationPhaseThree);

            Job EvaluationPhaseThree = Job.getInstance(configurationEvaluationPhaseThree,
                    "cs555_evaluation_phase_three");

            EvaluationPhaseThree.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            EvaluationPhaseThree.setNumReduceTasks(10);
            EvaluationPhaseThree.setMapperClass(MapperEvaluationPhaseThree.class);
//            EvaluationPhaseThree.setCombinerClass(CombinerEvaluationPhaseThree.class);
            EvaluationPhaseThree.setReducerClass(ReducerEvaluationPhaseThree.class);

            EvaluationPhaseThree.setMapOutputKeyClass(Text.class);
            EvaluationPhaseThree.setMapOutputValueClass(FloatSumAndCount.class);

            EvaluationPhaseThree.setOutputKeyClass(NullWritable.class);
            EvaluationPhaseThree.setOutputValueClass(FloatWritable.class);

            System.out.println("----Input of Evaluation-3 Job: " + outputpathEvaluationPhaseTwo + "----");
//            System.out.println("----Input of Evaluation-3 Job: " + testingPath + "----");
//            System.out.println("----Input of Cold Start-3 Job: " + outputpathUserSongs + "----");

            FileInputFormat.addInputPath(EvaluationPhaseThree, new Path(outputpathEvaluationPhaseTwo));
//            FileInputFormat.addInputPath(EvaluationPhaseThree, new Path(testingPath));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathEvaluationPhaseThree = actualOutputPath + "_evaluation_three";

            if (fileSystemForEvaluationPhaseThree
                    .exists(new Path(outputpathEvaluationPhaseThree))) {
                fileSystemForEvaluationPhaseThree
                        .delete(new Path(outputpathEvaluationPhaseThree), true);
            }

            FileOutputFormat.setOutputPath(EvaluationPhaseThree, new Path(
                    outputpathEvaluationPhaseThree));
            EvaluationPhaseThree.waitForCompletion(true);
            System.out.println("----Output Of EVALUATION-3 job: " + outputpathEvaluationPhaseThree
                    + "----");
        } else {
            outputpathEvaluationPhaseThree = actualOutputPath + "_evaluation_three";
        }

        if (runColdStartAnalysis) {
            System.out.println("----COLD START ANALYSIS----");
            Configuration configurationColdStartAnalysis = new Configuration();
            FileSystem fileSystemForColdStartAnalysis = FileSystem
                    .get(configurationColdStartAnalysis);

            Job ColdStartAnalysis = Job.getInstance(configurationColdStartAnalysis,
                    "cs555_coldstart_analysis");

            ColdStartAnalysis.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
            ColdStartAnalysis.setNumReduceTasks(10);
            ColdStartAnalysis.setMapperClass(MapperColdStartAnalysis.class);
//            EvaluationPhaseThree.setCombinerClass(CombinerEvaluationPhaseThree.class);
            ColdStartAnalysis.setReducerClass(ReducerColdStartanalysis.class);

            ColdStartAnalysis.setMapOutputKeyClass(Text.class);
            ColdStartAnalysis.setMapOutputValueClass(Text.class);

            ColdStartAnalysis.setOutputKeyClass(Text.class);
            ColdStartAnalysis.setOutputValueClass(Text.class);

            System.out.println("----Input of Cold start analysis Job: " + outputpathUserSimilarityThree + "----");
            System.out.println("----Input of Cold start analysis Job: " + outputpathUserSongs + "----");
//            System.out.println("----Input of Cold Start-3 Job: " + outputpathUserSongs + "----");

            FileInputFormat.addInputPath(ColdStartAnalysis, new Path(outputpathUserSimilarityThree));
            FileInputFormat.addInputPath(ColdStartAnalysis, new Path(outputpathUserSongs));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathColdStartAnalysis = actualOutputPath + "_coldstart_analysis";

            if (fileSystemForColdStartAnalysis
                    .exists(new Path(outputpathColdStartAnalysis))) {
                fileSystemForColdStartAnalysis
                        .delete(new Path(outputpathColdStartAnalysis), true);
            }

            FileOutputFormat.setOutputPath(ColdStartAnalysis, new Path(
                    outputpathColdStartAnalysis));
            ColdStartAnalysis.waitForCompletion(true);
            System.out.println("----Output Of Cold start analysis job: " + outputpathColdStartAnalysis
                    + "----");
        } else {
            outputpathColdStartAnalysis = actualOutputPath + "_coldstart_analysis";
        }

        if (runColdStartAnalysisTwo) {
            System.out.println("----COLD START ANALYSIS-2----");
            Configuration configurationColdStartAnalysisTwo = new Configuration();
            FileSystem fileSystemForColdStartAnalysisTwo = FileSystem
                    .get(configurationColdStartAnalysisTwo);

            Job ColdStartAnalysisTwo = Job.getInstance(configurationColdStartAnalysisTwo,
                    "cs555_coldstart_analysis_two");

            ColdStartAnalysisTwo.setJarByClass(AnalysisJob.class);
//            userSimilarityThreeJob.setInputFormatClass(NLineInputFormat.class);
//            userSimilarityThreeJob.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
//            ColdStartAnalysisTwo.setNumReduceTasks(10);
            ColdStartAnalysisTwo.setMapperClass(MapperColdStartAnalysisTwo.class);
//            EvaluationPhaseThree.setCombinerClass(CombinerEvaluationPhaseThree.class);
            ColdStartAnalysisTwo.setReducerClass(ReducerColdStartanalysisTwo.class);

            ColdStartAnalysisTwo.setMapOutputKeyClass(Text.class);
            ColdStartAnalysisTwo.setMapOutputValueClass(Text.class);

            ColdStartAnalysisTwo.setOutputKeyClass(Text.class);
            ColdStartAnalysisTwo.setOutputValueClass(Text.class);

            System.out.println("----Input of Cold start analysis - 2 Job: " + outputpathColdStartAnalysis + "----");
//            System.out.println("----Input of Cold start analysis - 2 Job: " + outputpathUserSongs + "----");
//            System.out.println("----Input of Cold Start-3 Job: " + outputpathUserSongs + "----");

            FileInputFormat.addInputPath(ColdStartAnalysisTwo, new Path(outputpathColdStartAnalysis));
//            FileInputFormat.addInputPath(ColdStartAnalysisTwo, new Path(outputpathUserSongs));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserSimilarityThree));
//            FileInputFormat.addInputPath(RecommendationsOne, new Path(outputpathUserCandidateSongsTwo));

            outputpathColdStartAnalysisTwo = actualOutputPath + "_coldstart_analysis_two";

            if (fileSystemForColdStartAnalysisTwo
                    .exists(new Path(outputpathColdStartAnalysisTwo))) {
                fileSystemForColdStartAnalysisTwo
                        .delete(new Path(outputpathColdStartAnalysisTwo), true);
            }

            FileOutputFormat.setOutputPath(ColdStartAnalysisTwo, new Path(
                    outputpathColdStartAnalysisTwo));
            ColdStartAnalysisTwo.waitForCompletion(true);
            System.out.println("----Output Of Cold start analysis -2 job: " + outputpathColdStartAnalysisTwo
                    + "----");
        } else {
            outputpathColdStartAnalysisTwo = actualOutputPath + "_coldstart_analysis_two";
        }
    }
}

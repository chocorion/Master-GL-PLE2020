import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointInputFormat extends InputFormat<LongWritable, Point2DWritable> {
    private static long numberOfSplit;
    private static long numberOfPointPerSplit;

    @Override
    public RecordReader<LongWritable, Point2DWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        return new RandomPointReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {
        ArrayList<InputSplit> list = new ArrayList<InputSplit>();

        for (int i = 0; i < numberOfSplit; i++) 
            list.add(new FakeInputSplit(RandomPointInputFormat.numberOfPointPerSplit));

        return list;
    }

    public static void setNumberOfSplit(long numberOfSplit) {
        RandomPointInputFormat.numberOfSplit = numberOfSplit;
    }

    public static void setNumberOfPointPerSplit(long numberOfPointPerSplit) {
        RandomPointInputFormat.numberOfPointPerSplit = numberOfPointPerSplit;
    }
    

}

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class RandomPointReader extends RecordReader<LongWritable, Point2DWritable> {
    private long numberOfPoints;
    private long progression;

    private Point2DWritable point = new Point2DWritable();

    private static Random random = new Random();

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(progression);
    }

    @Override
    public Point2DWritable getCurrentValue() throws IOException, InterruptedException {
        return point;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) ((double)progression / (double) numberOfPoints);
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        this.numberOfPoints = ((FakeInputSplit) arg0).getNumberOfPoints();
        this.progression = 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        point.setX(random.nextDouble());
        point.setY(random.nextDouble());

        progression += 1;

        return progression <= numberOfPoints;
    }   
}

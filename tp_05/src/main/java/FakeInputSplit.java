import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable {
    private long numberOfPoints;

    public FakeInputSplit() {}

    public FakeInputSplit(long numberOfPointPerSplit) {
        super();

        this.numberOfPoints = numberOfPointPerSplit;
    }

    public long getNumberOfPoints() {
        return this.numberOfPoints;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return numberOfPoints;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        this.numberOfPoints = arg0.readLong();

    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(this.numberOfPoints);

    }
    
}

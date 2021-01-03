import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point2DWritable implements Writable {
    public double x;
    public double y;

    public void readFields(DataInput arg0) throws IOException {
        this.x = arg0.readLong();
        this.y = arg0.readLong();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeDouble(this.x);
        arg0.writeDouble(this.y);
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }
}

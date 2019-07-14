package util;

import org.apache.hadoop.io.DoubleWritable;

public class DecDoubleWritable extends DoubleWritable {
    @Override
    public int compareTo(DoubleWritable o) {
        return -super.compareTo(o);
    }

    public DecDoubleWritable(double val) {
        super(val);
    }

    // bug fix: https://stackoverflow.com/questions/11446635/no-such-method-exception-hadoop-init
    public DecDoubleWritable(){}
}
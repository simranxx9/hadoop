package avg;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.*;
import java.io.*;

public class reducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable>{
    public void map(Text key, Iterator <FloatWritable> value, OutputCollector<Text, FloatWritable> values, Reporter r){
        float count = 0;
        float sum = 0;
        if(key.equals("M"))
        {
            while(value.hasNext())
            {
                sum += value.next().get();
                count++;
            }
        }
        else{
            while(value.hasNext())
            {
                sum += value.next().get();
                count++;
            }
        }
        values.collect(key, new FloatWritable(sum/count));
        values.collect(key, new FloatWritable(sum));
    }
}
package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	
	Integer count = 0;
	Integer sum_review = 0;
    
    for (IntWritable value : values) {
    	count++;
    	sum_review += value.get();
    }
    
    Double ave_review = (double) (sum_review / count);
    
    String res = count.toString() + "\t" + ave_review.toString();
    
    context.write(key, new Text(res));
  }
}
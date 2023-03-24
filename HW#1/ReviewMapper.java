package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text product_id = new Text();
	
	private IntWritable rating = new IntWritable();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  String[] str = line.split("\t");
	  
	  if (str[3].compareTo("product_id") != 0) {
			  product_id.set(str[3]);
			  rating.set(Integer.parseInt(str[7]));
	  }
	  
	  context.write(product_id, rating);  
  }
}
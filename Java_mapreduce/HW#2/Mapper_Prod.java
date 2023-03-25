package stubs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_Prod extends Mapper<LongWritable, Text, UserPairWritable, Text> 
{
	private Text product_id = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] tokens = line.split("\\W+");
		
		product_id.set(tokens[2]);
		UserPairWritable user_pairs = new UserPairWritable(tokens[0], tokens[1]);
		context.write(user_pairs, product_id);
	}

}

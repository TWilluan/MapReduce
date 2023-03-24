package stubs;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_Users extends Mapper <LongWritable, Text, Text, Text>
{
	private Text product_id = new Text();
	
	private Text user_id = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] tokens = line.split("\t");
		
		if (tokens[1].compareTo("customer_id") != 0 && Integer.parseInt(tokens[7]) >= 4) {
			product_id.set(tokens[3]);
			user_id.set(tokens[1]);
			context.write(product_id, user_id);
		}
	}
}

package stubs;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_Prod extends Reducer<UserPairWritable, Text, UserPairWritable, Text>{

	
	@Override
	public void reduce(UserPairWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException
	{
		boolean first = true;
		int count = 0;
		String res = "";
		for (Text value : values)
		{
			if (first) {
				res += value.toString();
				first = false;
			}
			else
				res += "," + value.toString();
			count++;
		}
		if (count >= 3)
			context.write(key, new Text(res));
	}
}

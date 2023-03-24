package stubs;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner_Users extends Reducer<Text, Text, Text, Text>{
	
	private Text users_text = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
	{
		StringBuilder users_str = new StringBuilder();
		ArrayList<String> lists = new ArrayList<String>();
		for (Text value : values)
		{
			String user = value.toString();
			if (!lists.contains(user)) {
				users_str.append(user).append(",");
				lists.add(user);
			}
		}
		
		users_text.set(users_str.substring(0,users_str.length()-1));
		context.write(key, users_text);
	}
}
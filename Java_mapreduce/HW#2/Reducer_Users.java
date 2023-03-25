package stubs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_Users extends Reducer<Text, Text, UserPairWritable, Text> {

	  @Override
	  public void reduce(Text key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {
		
		  for (Text value : values)
		  {
			  String list = value.toString();
			  String[] users = list.split(",");
			  
			  for (int i = 0; i < users.length-1; i++)
			  {
				  for (int j = i; j < users.length; j++)
				  {
					  if (users[i].compareTo(users[j]) != 0)
					  {
						  UserPairWritable pair;
						  if (users[i].compareTo(users[j]) < 0)
							   pair = new UserPairWritable(users[i], users[j]);
						  else
							   pair = new UserPairWritable(users[j], users[i]);
						  
						  
						  context.write(pair, key);
					  }
				  }
			  }
		  }
	  }
}
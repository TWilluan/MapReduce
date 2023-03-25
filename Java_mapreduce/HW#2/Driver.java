package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.conf.Configuration;

public class Driver {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 3) {
      System.out.printf("Usage: Driver <input dir> <output dir1> <output dir2>\n");
      System.exit(-1);
    }

    /*
     * Instantiate a Job object for your job's configuration. 
     */                                                                    
    Configuration conf1 = new Configuration();                              
    Job job1 = Job.getInstance(conf1, "Users");
    
    job1.setJarByClass(Driver.class);
    FileInputFormat.setInputPaths(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    
    
    job1.setMapperClass(Mapper_Users.class);
    job1.setCombinerClass(Combiner_Users.class);
    job1.setReducerClass(Reducer_Users.class);
    
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);    

    job1.setOutputKeyClass(UserPairWritable.class);
    job1.setOutputValueClass(Text.class);
    
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
//    boolean success1 = job1.waitForCompletion(true);
//    System.exit(success1 ? 0 : 1);
    job1.waitForCompletion(true);
    
    Configuration conf2 = new Configuration();                              
    Job job2 = Job.getInstance(conf2, "job2"); // Prods => job2

    job2.setJarByClass(Driver.class);    
    FileInputFormat.setInputPaths(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2])); // 1 => 2
    
    
    job2.setMapperClass(Mapper_Prod.class);
    job2.setReducerClass(Reducer_Prod.class);
    
    job2.setMapOutputKeyClass(UserPairWritable.class);
    job2.setMapOutputValueClass(Text.class);
    
    job2.setOutputKeyClass(UserPairWritable.class);
    job2.setOutputValueClass(Text.class);
    
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success2 = job2.waitForCompletion(true);
    System.exit(success2 ? 0 : 1);
  }
}
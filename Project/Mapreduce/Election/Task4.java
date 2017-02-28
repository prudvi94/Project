import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task4 {
public static class Mymapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
{
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		String[] line = value.toString().split(",");
	if(Integer.parseInt(line[0]) > 18 && line[7].equals("United-States") && line[8].equals("Foreign born- U S citizen by naturization") || line[8].equals("Native- Born abroad of American Parent(s)")){
		context.write(new Text("countryofbirth"),new DoubleWritable(1));	
	
	}	
	}
}
public static class Myreducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
{
	public void reduce(Text key,Iterable<DoubleWritable> value,Context context) throws IOException,InterruptedException
	{
		
		double sum=0;
		
		for(DoubleWritable val : value)
		{
	       sum= sum+val.get();
		}
		context.write(new Text(key),new DoubleWritable(sum));
				
			}
		}

public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException
{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf);
	job.setJarByClass(Task4.class);
	job.setMapperClass(Mymapper.class);
	job.setReducerClass(Myreducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(DoubleWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)? 0 : 1);
}
}

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




public class Tast2 {
	public static class Mymapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String[] line = value.toString().split(",");
			String token = line[3];
			if(line[2].equals("Never married") && line[8].equals("Native- Born in the United States")){
			
						
				context.write(new Text(token),new DoubleWritable(1));
		}}
	}
public static class Myreducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
{
	public void reduce(Text key,Iterable<DoubleWritable> value,Context context) throws IOException,InterruptedException
	{
		
		double sum=0;double sum1=0;
		
		
		for(DoubleWritable val : value)
		{
			if(key.toString().equals("Female"))
			{
			sum=sum+ val.get();
			}
			
			else
				{
				sum1=sum1+ val.get();
				}
			
			
			}
		if(sum>0)
		{
			context.write(new Text(key),new DoubleWritable(sum));	
		}
		else
		{
			context.write(new Text(key),new DoubleWritable(sum1));
		}
	
	}
}
public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException
{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf);
	job.setJarByClass(Tast2.class);
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




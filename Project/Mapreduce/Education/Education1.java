import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Education1 
{ 
	public static class Maptask1 extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key,Text values,Context context)throws IOException,InterruptedException
		{
			String[] str=values.toString().split(",");
			DoubleWritable lo=new DoubleWritable(Double.parseDouble(str[5]));
			{
			context.write(new Text("all"),lo);
			
			}
			if( str[1].equals("Bachelors degree(BA AB BS)") && Integer.parseInt(str[0]) > 18)
			{	context.write(new Text("shuffil"),lo);	
			
			}}
		}
	public static class Reducetask1 extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		public void reduce(Text key,Iterable<DoubleWritable> itr,Context context)throws IOException,InterruptedException
		{
			double sum=0;double per=0;int count=0;int count1=0; double sum1=0;
		for(DoubleWritable goog:itr)
		{
			if(key.toString().equals("all"))
			sum+=goog.get();
			count++;
			
		if(key.toString().equals("shuffil"))
			
			sum1=sum1+goog.get();
			count1++;
			
		per=(count1*100)/count;
		
		context.write(key,new DoubleWritable(per));
		context.write(key,new DoubleWritable(sum));
		}} }
	public static void main(String[] args)throws Exception
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"Stockmaxprice");
		job.setJarByClass(Education1.class);
		job.setMapperClass(Maptask1.class); //job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(Reducetask1.class);
		job.setOutputKeyClass(Text.class);//job.setInputFormatClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		} 
	} 
	
		
		
	
		
	

package project;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataEngineerJob {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		Text myKey = new Text();
		IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			
			String job_title = record[4];
			
			String year = record[7];
			
			if(job_title.contains("DATA ENGINEER") && job_title != null)
			{
				String str = "DATA ENGINEER"+","+year;
				
				myKey.set(str);
				
				context.write(myKey, one);
			}
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>
	{
		String[] years = {"2011","2012","2013","2014","2015","2016"};
		double[] arr = new double[6];
		TreeMap<String,Double> map = new TreeMap<String,Double>();
		int i = 0;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException 
		{
			int sum =0;
			
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			
			arr[i++] = sum;
			
		}
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException 
		{
			double avg = 0.0;
			double sum1 = 0.0;
			for(int i=0; i<6; i++ )
			{
				/*if(i == 0)
				{
					context.write(new Text(years[i]), new DoubleWritable(0));
				}
				else
				{
					context.write(new Text(years[i]), new DoubleWritable((arr[i]-arr[i-1])/arr[i-1]*100));
				}*/
				try {
					sum1 += (arr[i]-arr[i-1])/arr[i-1]*100;
					
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
				
			}
			avg = sum1 /5;
			context.write(new Text("Data Engineer Average Growth For Five Years"), new DoubleWritable(avg));
		}
		
		
		
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf,"Data Engineer Job Increasing");
		
		job.setJarByClass(DataEngineerJob.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}

}

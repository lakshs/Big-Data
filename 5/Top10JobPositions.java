package project;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Top10JobPositions {

	public static class JobMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			String year = record[7];
			String job_title = record[4];
			
			context.write(new Text(job_title), new Text(year));
		}
		
	}
	
	public static class YearPartitioner extends Partitioner<Text, Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			
			String year = value.toString();
			
			if(year.equals("2011"))
			{
				return 0 % numReduceTasks;
			}
			else if(year.equals("2012"))
			{
				return 1 % numReduceTasks;
			}
			else if(year.equals("2013"))
			{
				return 2 % numReduceTasks;
			}
			else if(year.equals("2014"))
			{
				return 3 % numReduceTasks;
			}
			else if(year.equals("2015"))
			{
				return 4 % numReduceTasks;
			}
			else
			{
				return 5 % numReduceTasks;
			}
		}
		
	}
	
	public static class JobReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		TreeMap<Integer, Text> map = new TreeMap<Integer, Text>();

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			int count = 0;
			String year = "";
			for(Text val : values)
			{
				 year = val.toString();
				 count++;
			}
			
			String Job_title = key.toString();
			String myValue = year+","+Job_title +","+count;
			
			map.put(new Integer(count), new Text(myValue));
			if(map.size() > 10)
			{
				map.remove(map.firstKey());
			}
		}

		@Override
		protected void cleanup( Context context)throws IOException, InterruptedException 
		{
			for(Text top10 : map.descendingMap().values())
			{
				context.write(NullWritable.get(), top10);
			}
		}
		
		
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Top 10 Job Positios for each Year");
		
		job.setJarByClass(Top10JobPositions.class);
		
		job.setMapperClass(JobMapper.class);
		
		job.setPartitionerClass(YearPartitioner.class);
		job.setNumReduceTasks(6);
		
		job.setReducerClass(JobReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

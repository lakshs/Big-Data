package project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Q8A_AvgPrWageForEachJobForEachYearFullTime {



	
	public static class AvgPreWageMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String mySearchText = context.getConfiguration().get("myText");
			
			String[] record = value.toString().split("\t");
			
			String case_status = record[1];
			String year = record[7];
			String job_title = record[4];
			String full_time_position = record[5];
			String prevailing_wage = record[6];
			
			
			if(mySearchText.equals("ALL"))
			{
			  if(full_time_position.equals("Y"))
			{
				
				    if(case_status.equals("CERTIFIED") || case_status.equals("CERTIFIED-WITHDRAWN"))
				{
					String myValue = year+"\t"+prevailing_wage;
					
					context.write(new Text(job_title), new Text(myValue));
				}
			}
		
		  }
			
			else
			{
				if(full_time_position.equals("Y") && year.equals(mySearchText))
				{
					
					    if(case_status.equals("CERTIFIED") || case_status.equals("CERTIFIED-WITHDRAWN"))
					{
						String myValue = year+"\t"+prevailing_wage;
						
						context.write(new Text(job_title), new Text(myValue));
					}
				}
			}
			
			
		}	
	}
	
	public static class YearPartitioner extends Partitioner<Text, Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			
			String[] token = value.toString().split("\t");
			String year = token[0];
			
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
	
	public static class AvgPreWageReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			long count = 0;
			long total = 0;
			String year = "";
			for (Text val : values)
			{
				String[] str = val.toString().split("\t");
				year = str[0];
				long prevailing_wage = Long.parseLong(str[1]); 
				
				count++;
				total = total+prevailing_wage;
			}
			
			String job_title = key.toString();
			
			double average_prevailing_wage = total/count;
			
			String myKey = job_title+"\t"+year;
			
			context.write(new Text(myKey), new DoubleWritable(average_prevailing_wage));
			
		}
		
	}
	
	
	public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
	{
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			
			String job_title = record[0];
			String year = record[1];
			double average_prevailing_wage = Double.parseDouble(record[2]);
			String myVal =year+"\t"+job_title;
			
			context.write(new DoubleWritable(average_prevailing_wage), new Text(myVal));
		}	
	}
	
	public static class SortYearPartitioner extends Partitioner<DoubleWritable, Text>
	{

		@Override
		public int getPartition(DoubleWritable key, Text value, int numReduceTasks) {
			
			String[] token = value.toString().split("\t");
			String year = token[0];
			
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
	
	public static class SortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
	{
		int counter = 0;
		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			
			for(Text val :values)
			{
				if(counter < 5)
				{	
				 context.write(new Text(val), key);
				 counter= counter+1;
				}
			}
		}	
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		 if(args.length > 2)
		  {
			  
			   conf.set("myText", args[2]);  
		  }
		  
		
		Job job1 = Job.getInstance(conf, "Average Prevailing Wage for each Job for each Year");
		
		job1.setJarByClass(Q8A_AvgPrWageForEachJobForEachYearFullTime.class);
		
		job1.setMapperClass(AvgPreWageMapper.class);
		
		if(args[2].equals("ALL"))
			  
		{
		   job1.setPartitionerClass(YearPartitioner.class);
		   job1.setNumReduceTasks(6);
		}
		
		job1.setReducerClass(AvgPreWageReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(DoubleWritable.class);
		job1.setOutputValueClass(Text.class);
		
		Path outputpath1 = new Path("FirstMapper2");
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, outputpath1);
		
		FileSystem.get(conf).delete(outputpath1, true);
		job1.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance(conf, "Average Prevailing Wage for each Job for each Year");
		
		job2.setJarByClass(Q8A_AvgPrWageForEachJobForEachYearFullTime.class);
		
		job2.setMapperClass(SortMapper.class);
		

		if(args[2].equals("ALL"))
			  
		{
		job2.setPartitionerClass(SortYearPartitioner.class);
		job2.setNumReduceTasks(6);
		
		}
		
		job2.setSortComparatorClass(DecreasingComparator.class);
		
		job2.setReducerClass(SortReducer.class);
		
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job2, outputpath1);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		FileSystem.get(conf).delete(new Path(args[1]), true);
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}



}

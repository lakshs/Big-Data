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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Question3_TreeMap {

	public static class MapperClass extends Mapper <LongWritable, Text, Text, LongWritable>
	{

		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			
			String[] str = value.toString().split("\t");
			String soc_name = str[3];
			String job = str[4];
			String status = str[1];
		    if (job.contains("DATA SCIENTIST") && status.equals("CERTIFIED"))
		    {
		    	context.write(new Text(soc_name), new LongWritable(1));
		    }
		    
		    
		}
	}
	
	 //Reducer Class
	
		public static class ReducerClass extends Reducer<Text, LongWritable,NullWritable,Text>
		{
			TreeMap<Long,Text> record = new TreeMap<Long,Text>();
			
			public void reduce(Text key, Iterable<LongWritable> values, Context context)throws IOException, InterruptedException
			{
				int sum = 0;
				String myValue = "";
				String mySum = "";
				
				for(LongWritable val : values)
				{
					sum += val.get();
				}
				
				myValue = key.toString();
				mySum = String.format("%d", sum);
				myValue = myValue + ',' + mySum;
				
				record.put(new Long(sum),new Text(myValue));
				
				if(record.size() > 1)
				{
					record.remove(record.firstKey());
				}
			}
		
		  
		 protected void cleanup(Context context) throws IOException,InterruptedException
		 {
			 for(Text t : record.descendingMap().values())
			 {
				 context.write(NullWritable.get(), t);
			 }
		 }
		
		
	  }
		
		
		// Main Class
		
		public static void main(String args[]) throws Exception
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Top Data Scientist Position");
			job.setJarByClass(Question3_TreeMap.class);
			
			job.setMapperClass(MapperClass.class);
			job.setReducerClass(ReducerClass.class);
		
			job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	       System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		
		
	}

	
	

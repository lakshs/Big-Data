package project;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;

public class JobSuccessRate 
{
  
	public static class MapperClass extends Mapper <LongWritable, Text, Text, Text>
	{

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String parts[] =value.toString().split("\t");
			String status = parts[1];
			String jobposition =parts[4].replaceAll("\"", "");
			context.write(new Text(jobposition), new Text(status));
		}
		
	}
	
	
	public static class ReducerClass extends Reducer <Text,Text,NullWritable,Text>
	{

		private TreeMap<Double, String> topten = new TreeMap<>();
		public void reduce(Text key, Iterable<Text> value, Context context){
			 double total =0;
			 double successrate=0;
			 for (Text val:value)
			 {
				 String status = val.toString();
				 if(status.equals("CERTIFIED") || status.equals("CERTIFIED WITHDRAWN"))
				 {
					total++ ;
				    successrate++;
				 }
				    else 
				    	total++;
			 		}
			double rate = (successrate/total)*100;
			if(rate >=70 && total >=1000){
				String op = key.toString()+ "@"+String.format("%.0f",total)+"@" + String.format("%.2f %%",rate);
			
			topten.put(rate, op);
			}
			
				 
		}
		protected void cleanup(Context context) throws IOException, InterruptedException{
			for(String val : topten.values()){
				context.write(NullWritable.get(),new Text(val));
			}
				
		
		}
		
	}
	
	public static void main(String[] args ) throws Exception
	{
		Configuration conf =new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job=Job.getInstance(conf);
		job.setJarByClass(JobSuccessRate.class);
		
		job.setMapperClass(MapperClass.class);
		
		
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	    
	}

}

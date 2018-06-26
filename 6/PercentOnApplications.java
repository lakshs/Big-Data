package project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PercentOnApplications {

	
	//Mapper Class
	public static class PercentMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			String case_status = record[1];
			String year = record[7];
			
			context.write(new Text(year), new Text(case_status));
		}
		
	}
	
	
	 //Reducer Class
	public static class PercentReducer extends Reducer<Text, Text, Text, Text>
	{

		double allcount = 0;
		double certified = 0;
		double denied = 0;
		double withdrawn1 = 0;
		double certified_withdrawn = 0;
		
		double cert_percent = 0.0;
		double certwith__percent =0.0;
		double with_percent = 0.0;
		double deni_percent = 0.0;
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			double allcount = 0;
			double certified = 0;
			double denied = 0;
			double withdrawn1 = 0;
			double certified_withdrawn = 0;
			
			
			double cert_percent = 0.0;
			double certwith__percent =0.0;
			double with_percent = 0.0;
			double deni_percent = 0.0;
			
			
			String case_status = "";
			
			for(Text val : values)
			{
				
				case_status = val.toString();
				 
				allcount++;
				
				if(case_status.equals("CERTIFIED-WITHDRAWN"))
				{
					certified_withdrawn++;
				}
				if(case_status.equals("CERTIFIED"))
				{
					certified++;
				}
			    if(case_status.equals("WITHDRAWN"))
				{
					withdrawn1++;
				}
				if(case_status.equals(("DENIED")))
				{
					denied++;
				}
				
				
				
			}
			
			
			 cert_percent = (certified * 100)/allcount;
			 certwith__percent = (certified_withdrawn*100)/allcount;
			 with_percent = (withdrawn1*100)/allcount;
			 deni_percent = (denied*100)/allcount;
			
			String myValue = String.format("%f", cert_percent) +","+String.format("%f", certwith__percent)+","+String.format("%f", with_percent)+","+String.format("%f", deni_percent);
			
			// String myValue = cert_percent+","+certwith__percent+","+with_percent+","+deni_percent;
			context.write(key, new Text(myValue));
		}
	}
	
	
	    //Main Method
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			
			conf.set("mapreduce.output.textoutputformat.separator",",");
			
			Job job = Job.getInstance(conf,"Percentage and Count of each case status for each year");
			job.setJarByClass(PercentOnApplications.class);
			
			job.setMapperClass(PercentMapper.class);
			job.setReducerClass(PercentReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

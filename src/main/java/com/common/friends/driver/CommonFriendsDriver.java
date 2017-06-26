package com.common.friends.driver;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.common.friends.mr.commonFriendsMapper;
import com.common.friends.mr.commonFriendsReducer;


public class CommonFriendsDriver extends Configured implements Tool{
	private static final Logger log = Logger.getLogger(CommonFriendsDriver.class);

	
	public static void main(String[] argsAll) throws Exception{
		
		try{
			Configuration conf = new Configuration();
			
			
			
			int retVal = ToolRunner.run(conf,new CommonFriendsDriver(),argsAll);
		}catch(Exception ex){
			ex.printStackTrace();
			log.error("Job Failed " +ex.getMessage() );
			throw ex;
		}
	}
	
	@Override
	public int run(String[] argsAll) throws Exception {
		
		GenericOptionsParser gop = new GenericOptionsParser(argsAll);
		String[] args = gop.getRemainingArgs();
		
		String friends_input_path = args[0]; //should contain the word pcs in the path
		log.info("friends input received "+ args[0]);
		String output_path    = args[1];//output path
		log.info("output location "+ args[1]);
		getConf().set("fs.defaultFS", argsAll[2]);
		log.info("fs system Default  "+ args[2]);
		getConf().set("USERlIST",argsAll[3]);
		String input_path = friends_input_path;

		Job job = Job.getInstance(getConf(),"CommonFriendsDriver");
		
		job.setJarByClass(CommonFriendsDriver.class);
		
		job.setMapperClass(commonFriendsMapper.class);
//		job.setReducerClass(commonFriendsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPaths(job, input_path);
		
		Path outputpath = new Path(output_path);
		FileSystem fs = FileSystem.get(getConf());
		
		if(fs.exists(outputpath)){
			fs.delete(outputpath, true);
		}
		
		
		TextOutputFormat.setOutputPath(job, outputpath);
//		MultipleOutputs.addNamedOutput(job, "AdvertiserIdLevel", TextOutputFormat.class, NullWritable.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, "LineItemIdLevel", TextOutputFormat.class, NullWritable.class, Text.class);
//		MultipleOutputs.addNamedOutput(job, "OrderIdLevel", TextOutputFormat.class, NullWritable.class, Text.class);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	
	
//		job.addCacheFile(new URI(advertiserMakeMap));
//		job.addCacheFile(new URI(orderIDToMakeMap));
		
	
		
		boolean retVal = job.waitForCompletion(true);
		
		return 0;
	}

}

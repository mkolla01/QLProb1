package com.common.friends.mr;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class commonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Logger log = Logger.getLogger(commonFriendsMapper.class);

	String sourceforrcd = "";
	StringBuffer sbf = new StringBuffer();
	Text outkey = new Text();
	Text outval = new Text();
	StringBuffer outkeysb = new StringBuffer();
	String[] valArr = null;
	String inputdelimiter = ",";
	// String inputdelimiter = "\t";

	String outputdelimiter = "\t";
	String[] friendsList;


	Context context;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		super.setup(context);
		this.context = context;
	}
	
	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
	

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
	   String make = null;
		try {

			sbf.setLength(0);
			outkeysb.setLength(0);			
			context.getCounter("Count", "TotalRecordsRead").increment(1);

			String valuestr = value.toString();
			valArr = valuestr.split(inputdelimiter, -1);

			sbf.append(valuestr);
			
			for(int i=1;i<valArr.length;i++) {
				String outKey;
				String str = valArr[i];
				if ( str.compareTo(valArr[0]) > 0) {
					outKey = valArr[0] + "_" + str;
				} else {
					outKey =  str + "_" + valArr[0];
				}
				outkey.clear();
				outkey.set(outKey);		
				outval.clear();
				// sbf.append(valuestr);
				outval.set(valuestr);
				context.write(outkey, outval);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			context.getCounter("Count", "MapperExceptionCount").increment(1);
			//throw ex;
		}
	}
}

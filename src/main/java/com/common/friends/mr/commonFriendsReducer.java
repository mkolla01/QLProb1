package com.common.friends.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.log4j.Logger;





public class commonFriendsReducer extends Reducer<Text, Text, Text, Text> {

	private static final Logger log = Logger.getLogger(commonFriendsReducer.class);
	StringBuffer outputvaluebf = new StringBuffer();

	Text outputText = new Text();
	Context context;

	private String outputPath;

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		BufferedReader br = null;
		try{
			super.setup(context);

			this.context = context;

		}catch(Exception ex){
			throw ex;
		}finally{
			try{
				if(br != null)
					br.close();
			}catch(Exception ex){
				throw ex;
			}
		}

	}

	String inputdelimiter = ",";

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		HashSet<String> firstSetFriends = new HashSet<String>();
		String userList = context.getConfiguration().get("USERlIST").replace("\"", "");
		HashSet<String> hsSet = getResultKeySet(userList);
		String[] friendsdsKey = key.toString().split("_");
		String[] valArry = new String[2];
		valArry[0] = null;
		valArry[1] = null;
		int i=0;
		for(Text value:values){
			valArry[i] = value.toString();
			i++;
		}
		String[] firstFriendsList = valArry[0].split(inputdelimiter);
		if (valArry[1] != null) {
			String[] secondFriendsList = valArry[1].split(inputdelimiter);
			for (int k = 0; k < firstFriendsList.length; k++) {
				firstSetFriends.add(firstFriendsList[k]);
			}

			StringBuffer commonFriends = new StringBuffer();
			boolean firstCommonFriend = true;
			for (int k = 0; k < secondFriendsList.length; k++) {
				if (firstSetFriends.contains(secondFriendsList[k])
						&& ( !secondFriendsList[k].equalsIgnoreCase(friendsdsKey[0])
								&& !secondFriendsList[k].equalsIgnoreCase(friendsdsKey[1]))
						) {
					if (!firstCommonFriend) {
						commonFriends.append(',');
					}
					commonFriends.append(secondFriendsList[k]);
					firstCommonFriend = false;
				}
			}

			Text outputCountVal = new Text();
			outputCountVal.clear();
			outputCountVal.set(commonFriends.toString());
			if (hsSet.contains(key.toString()) ) {
			    context.write(key, outputCountVal);
			}
		}
	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);

	}

	private HashSet<String> getResultKeySet(String userList) {
	    HashSet<String> hsSet = new HashSet<String>();
	    String[] usrArry = userList.split("->");
	    String[] frndsArry = usrArry[1].split(",");
	    for ( int i =0 ;i<frndsArry.length;i++) {
	        hsSet.add(usrArry[0] + "_" + frndsArry[i]);
	        hsSet.add(frndsArry[i] + "_" + usrArry[0]);
	    }
	    return hsSet;
	}

}

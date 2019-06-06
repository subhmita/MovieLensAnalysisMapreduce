package com.hadoop.MovieLensKPI3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingJoinReducer  extends Reducer<Text,Text,Text,Text> {
	
	private ArrayList<Text> listUser = new ArrayList<Text>();
	private ArrayList<Text> listRating = new ArrayList<Text>();

	@Override
	public void reduce (Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		listUser.clear();
		listRating.clear();
		for(Text text :values) {
			if (text.charAt(0) == 'U') {
				listUser.add(new Text(text.toString().substring(1)));
				} else if (text.charAt(0) == 'R') {
				listRating.add(new Text(text.toString().substring(1)));
				}
				}	
		executeJoinLogic(context);
	}
	
	private void executeJoinLogic(Context context) throws IOException, InterruptedException {
		
		if (!listUser.isEmpty() && !listRating.isEmpty()) {
			for (Text userData : listUser) {
				
				for (Text ratingData :listRating){
					context.write(userData, ratingData);
				}
			}
		}
	}
}
			 


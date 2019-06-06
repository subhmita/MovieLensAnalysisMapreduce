package com.hadoop.MovieLensKPI2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingJoinReducer  extends Reducer<Text,Text,Text,Text> {
	
	private ArrayList<Text> listMovies = new ArrayList<Text>();
	private ArrayList<Text> listRating = new ArrayList<Text>();

	@Override
	public void reduce (Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		listMovies.clear();
		listRating.clear();
		for(Text text :values) {
			if (text.charAt(0) == 'M') {
				listMovies.add(new Text(text.toString().substring(1)));
				} else if (text.charAt(0) == 'R') {
				listRating.add(new Text(text.toString().substring(1)));
				}
				}	
		executeJoinLogic(context);
	}
	
	private void executeJoinLogic(Context context) throws IOException, InterruptedException {
		int sumRating=0;
		if (!listMovies.isEmpty() && !listRating.isEmpty()) {
			for (Text moviesData : listMovies) {
				if(listRating.size()>40) {
					for(Text x :listRating) {
						int value = Integer.parseInt(x.toString()); 
						sumRating = sumRating +value;
					}
					context.write(moviesData, new Text(String.valueOf(sumRating)));
				}
			}
		}
	}
}
			 


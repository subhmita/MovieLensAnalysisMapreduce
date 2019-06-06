package com.hadoop.MovieLensKPI3;

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
			} else if (text.charAt(0) == 'C') {
				listRating.add(new Text(text.toString().substring(1)));
			}
		}	
		executeJoinLogic(context);
	}

	private void executeJoinLogic(Context context) throws IOException, InterruptedException {
		if (!listMovies.isEmpty() && !listRating.isEmpty()) {
			for (Text moviesData : listMovies) {
				String[] data = moviesData.toString().split("::");
				String[] genres=data[1].split("\\|");
				for(Text x :listRating) {
					for(String genre :genres) {
						context.write(new Text(genre),x);
					}
				}				
			}
		}
	}
}



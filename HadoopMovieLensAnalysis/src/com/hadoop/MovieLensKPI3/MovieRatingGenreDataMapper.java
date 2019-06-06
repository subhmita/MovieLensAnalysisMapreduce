package com.hadoop.MovieLensKPI3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatingGenreDataMapper extends Mapper<Object,Text,Text,Text> {

	private Text movieID = new Text();
	private Text outvalue = new Text();
	@Override
	public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {
//		MovieID::Title::Genres
//		1::Toy Story (1995)::Animation|Children's|Comedy
		
		String line = value.toString();
		String[] lineparts = line.split("::");	
		
		String sMovieId =lineparts[0];
		movieID.set(sMovieId);
		outvalue.set("M"+lineparts[1]+ "::" + lineparts[2]);
		context.write(movieID,outvalue);

	}
}

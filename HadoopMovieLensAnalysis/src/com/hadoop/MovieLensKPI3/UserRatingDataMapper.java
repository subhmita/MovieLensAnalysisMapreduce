package com.hadoop.MovieLensKPI3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserRatingDataMapper extends Mapper<Object,Text,Text,Text> {

	private Text movieID = new Text();
	private Text outvalue = new Text();
	@Override
	public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {
//		18-35::academic/educator::1091::3
//		18-35::academic/educator::2470::5
		
		String line = value.toString();
		String[] lineparts = line.split("::");	
		
		String sMovieId =lineparts[2];
		movieID.set(sMovieId);
		outvalue.set("C"+lineparts[0]+ "::" + lineparts[1]+ "::" + lineparts[3]);
		context.write(movieID,outvalue);

	}
}

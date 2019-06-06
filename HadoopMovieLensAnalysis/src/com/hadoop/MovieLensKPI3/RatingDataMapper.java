package com.hadoop.MovieLensKPI3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingDataMapper extends Mapper<Object,Text,Text,Text>{
	
	private Text userId = new Text();
	private Text outvalue = new Text();
@Override
public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {

//	UserID::MovieID::Rating::Timestamp
//	1::1193::5::978300760
	String line = value.toString();
	String[] lineparts = line.split("::");	
	if(lineparts.length>1) {
	String sUserId = lineparts[0];
	userId.set(sUserId);
	String sMovieId = lineparts[1];
	String sRating = lineparts[2];
	outvalue.set("R"+sMovieId+"::"+sRating);
	context.write(userId,outvalue);
	}
}
	
}

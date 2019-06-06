package com.hadoop.MovieLensKPI2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<Object,Text,Text,Text>{
	
@Override
public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {

//	1::1193::5::978300760
	String line = value.toString();
	String[] lineparts = line.split("::");	
	if(lineparts.length>1) {
	String sMovieId = lineparts[1];
	String sRating = lineparts[0];
	context.write(new Text(sMovieId), new Text("R"+sRating));
	}
}
	
}

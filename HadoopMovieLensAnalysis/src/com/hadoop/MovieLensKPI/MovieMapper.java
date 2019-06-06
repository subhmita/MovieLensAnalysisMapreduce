package com.hadoop.MovieLensKPI;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<Object,Text,Text,Text>{
	
@Override
public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {

//	1::Toy Story (1995)::Animation|Children's|Comedy
	String line = value.toString();
	String[] lineparts = line.split("::");	
	if(lineparts.length>1) {
	String sMovieId = lineparts[0];
	String sTitle = lineparts[1];
	context.write(new Text(sMovieId), new Text("M"+sTitle));
	}
}
	
}

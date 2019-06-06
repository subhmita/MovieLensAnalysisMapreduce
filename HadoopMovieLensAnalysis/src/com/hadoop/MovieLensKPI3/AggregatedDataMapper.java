package com.hadoop.MovieLensKPI3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregatedDataMapper extends Mapper<Object,Text,Text,Text> {

	private Text key = new Text();
	private Text outvalue = new Text();
	@Override
	public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {
//		Animation::18-35::programmer::5
//		Children’s::18-35::programmer::5
//		Comedy::18-35::programmer::5
		
		String line = value.toString();
		String[] lineparts = line.split("::");			
		key.set(lineparts[2]+"::"+lineparts[1]);
		outvalue.set(lineparts[0]+"::"+lineparts[3]);
		context.write(key,outvalue);

	}
}

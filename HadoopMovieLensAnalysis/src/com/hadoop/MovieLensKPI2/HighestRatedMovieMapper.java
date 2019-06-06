package com.hadoop.MovieLensKPI2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HighestRatedMovieMapper extends Mapper<Object, Text, NullWritable, Text> {
	
	private TreeMap<Integer, Text> highestRated = new TreeMap<Integer, Text>();
	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
	String data = values.toString();
	String[] field = data.split("::", -1);
	if (null != field && field.length == 2) {
	int rating = Integer.parseInt(field[1]);
	highestRated.put(rating, new Text(field[0]+"::"+field[1]));
	if (highestRated.size() > 20) {
	highestRated.remove(highestRated.firstKey());
	}
	}
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
	for (Map.Entry<Integer, Text> entry : highestRated.entrySet()) {
	context.write(NullWritable.get(), entry.getValue());
	}
	}
	}

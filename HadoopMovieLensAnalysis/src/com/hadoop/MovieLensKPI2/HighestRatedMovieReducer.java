package com.hadoop.MovieLensKPI2;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestRatedMovieReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Integer, Text> highestRated = new TreeMap<Integer, Text>();
	@Override
	public void reduce (NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		for(Text value :values) {
			String data = value.toString();
			String[] field = data.split("::", -1);
			if (field.length == 2) {
				highestRated.put(Integer.parseInt(field[1]), new Text(value));
				if (highestRated.size() > 20) {
			    highestRated.remove(highestRated.firstKey());
		}
	}
	}
		for (Text t : highestRated.descendingMap().values()) {
			context.write(NullWritable.get(), t);
			}
			}

}

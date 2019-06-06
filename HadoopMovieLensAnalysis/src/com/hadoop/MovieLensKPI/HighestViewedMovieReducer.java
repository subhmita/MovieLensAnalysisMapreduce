package com.hadoop.MovieLensKPI;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestViewedMovieReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Integer, Text> highestView = new TreeMap<Integer, Text>();
	@Override
	public void reduce (NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		for(Text value :values) {
			String data = value.toString();
			String[] field = data.split("::", -1);
			if (field.length == 2) {
				highestView.put(Integer.parseInt(field[1]), new Text(value));
				if (highestView.size() > 10) {
			    highestView.remove(highestView.firstKey());
		}
	}
	}
		for (Text t : highestView.descendingMap().values()) {
			context.write(NullWritable.get(), t);
			}
			}

}

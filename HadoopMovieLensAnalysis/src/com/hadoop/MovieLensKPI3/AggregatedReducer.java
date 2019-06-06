package com.hadoop.MovieLensKPI3;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class AggregatedReducer extends Reducer<Text, Text, Text, Text> {
	
	private Map<String, GenreRanking> map = new HashMap<String, GenreRanking>();
	private TreeMap<Double, String> finalMap = new TreeMap<Double, String>();
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		map.clear();
		finalMap.clear();
		for (Text text : values) {
			String[] value = text.toString().split("::");
			String genre = value[0];
			double rating = Double.parseDouble(value[1]);
			GenreRanking ranking = map.get(genre);
			if (ranking != null) {
				ranking.setSum(ranking.getSum() + rating);
				ranking.setCount(ranking.getCount() + 1);
			} else {
				GenreRanking rankingNew = new GenreRanking();
				rankingNew.setSum(rating);
				rankingNew.setCount(1);
				rankingNew.setGenre(genre);
				map.put(genre, rankingNew);
			}
		}
		for (Map.Entry<String, GenreRanking> entry : map.entrySet()) {
			GenreRanking gr = entry.getValue();
			double average = gr.getSum() / gr.getCount();
			finalMap.put(average, entry.getKey());
		}
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for (Map.Entry<Double, String> entry : finalMap.descendingMap().entrySet()) {
			if (count < 5) {
				sb.append(entry.getValue() + "::");
				count++;
			} else {
				break;
			}
		}
		context.write(key, new Text(sb.toString().substring(0, sb.toString().lastIndexOf("::"))));
	}
}
package com.hadoop.MovieLensKPI2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MovieLensDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherargs.length != 4) {
			System.err.println("Invalid Command");
			System.err.println("Usage WordCount Input Path Output Path");
			System.exit(0);
		}
		Job job = new Job(conf,"cpucount");
		job.setJarByClass(MovieLensDriver.class);
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		//get the FileSystem, you will need to initialize it properly		
		
		job.setReducerClass(MovieRatingJoinReducer.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass( Text.class);
		job.setOutputValueClass(Text.class);	
		int code = job.waitForCompletion(true) ? 0 : 1;
		if (code == 0) {
			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(MovieLensDriver.class);
			job2.setJobName("Highest_Viewed");
			job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			job2.setMapperClass(HighestRatedMovieMapper.class);
			job2.setReducerClass(HighestRatedMovieReducer.class);
			job2.setNumReduceTasks(1);
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
			}
			}
			}
package com.hadoop.MovieLensKPI3;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
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
		if(otherargs.length != 7) {
			System.err.println("Invalid Command");
			System.err.println("Usage WordCount Input Path Output Path");
			System.exit(0);
		}
		/* delete the output directory before running the job */
		FileUtils.deleteDirectory(new File(args[2]));
		conf.set("id.to.profession.mapping.file.path", args[5]);

		Job job = new Job(conf,"cpucount");
		job.setJarByClass(MovieLensDriver.class);
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		//get the FileSystem, you will need to initialize it properly		
		
		job.setReducerClass(UserRatingJoinReducer.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserDataMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingDataMapper.class);
		
		job.setOutputKeyClass( Text.class);
		job.setOutputValueClass(Text.class);	
		int code=job.waitForCompletion(true) ? 0 : 1;
		int code2 = 1;
		if (code == 0) {
			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(MovieLensDriver.class);
			job2.setJobName("Highest_Viewed");
			job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class,
					UserRatingDataMapper.class);
			MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, MovieRatingGenreDataMapper.class);		
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			job2.setReducerClass(MovieRatingJoinReducer.class);
			job2.setNumReduceTasks(1);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			TextOutputFormat.setOutputPath(job2,new Path(args[3]));
			code2=job2.waitForCompletion(true) ? 0 : 1;
			}
		if (code2 == 0) {
			Job job3 = Job.getInstance(conf);
			job3.setJarByClass(MovieLensDriver.class);
			job3.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			job3.setJobName("Find_Highest_Rank");
			FileInputFormat.addInputPath(job3, new Path(args[3]));
			FileOutputFormat.setOutputPath(job3, new Path(args[6]));
			job3.setMapperClass(AggregatedDataMapper.class);
			job3.setReducerClass(AggregatedReducer.class);
			job3.setNumReduceTasks(1);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			System.exit(job3.waitForCompletion(true) ? 0 : 1);
		}	

			}
			}
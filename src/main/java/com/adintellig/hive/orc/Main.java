package com.adintellig.hive.orc;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import com.adintellig.hive.orc.mapred.ORCMapper;
import com.hadoop.mapred.DeprecatedLzoTextInputFormat;

/**
 * 
 * Must use old package of MapReduce: org.apache.hadoop.mapred.*.
 */
public class Main {

	public static final String JOB_NAME = "Hive ORC Generation";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		String input = cmd.getOptionValue("i");
		String output = cmd.getOptionValue("o");
		String date = cmd.getOptionValue("d");
		String hour = cmd.getOptionValue("h");
		String kind = cmd.getOptionValue("k");
		String queue = cmd.getOptionValue("q");

		// valid
		int dateInt = Integer.parseInt(date);
		if (dateInt < 19700101 || dateInt > 21000101)
			throw new Exception("date format invalid: " + date);
		int hourInt = Integer.parseInt(hour);
		if (hourInt < 0 || hourInt > 24)
			throw new Exception("hour format invalid: " + hour);

		// config
		String prefix = genOutputFileNamePrefix(kind, date, hour);
		if (null != prefix && prefix.trim().length() > 0)
			conf.set("hive.orc.output.filename.prefix", prefix.trim());
		if (null != kind && kind.trim().length() > 0)
			conf.set("hive.orc.input.log.kind", kind.trim().toLowerCase());
		if (null != queue && queue.trim().length() > 0)
			conf.set("mapred.job.queue.name", queue.trim());

		JobConf job = new JobConf(conf);
		job.setJobName(JOB_NAME);
		job.setJarByClass(Main.class);
		job.setMapperClass(ORCMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setInputFormat(DeprecatedLzoTextInputFormat.class);
		job.setOutputFormat(OrcOutputFormat.class);

		DeprecatedLzoTextInputFormat.addInputPath(job, new Path(input));

		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(output, hour);
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		OrcOutputFormat.setOutputPath(job, outputPath);

		JobClient.runJob(job);

//		Path mergedOutputPath = new Path(output, prefix + "_part-00000.orc");
		Path movedOutputPath = new Path(output);

		System.out.println("[copy merge]: " + outputPath.toString() + " -> "
				+ movedOutputPath.toString());

//		FileUtil.copyMerge(fs, outputPath, fs, mergedOutputPath, true, conf,
//				null);

		List<Path> list = new ArrayList<Path>();
		FileStatus[] contents = fs.listStatus(outputPath);
		for (int i = 0; i < contents.length; i++) {
			if (!contents[i].isDir()) {
				list.add(contents[i].getPath());
			}
		}
		Path[] newPath = (Path[])list.toArray();
		FileUtil.copy(fs, newPath, fs, movedOutputPath, true, true, conf);
	}

	private static String genOutputFileNamePrefix(String kind, String date,
			String hour) {
		return kind.toLowerCase() + "_" + date + "_" + hour;
	}

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("i", "input", true,
				"the directory or file to read from (must exist)");
		o.setArgName("input");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("o", "output", true, "output directory (must exist)");
		o.setArgName("output");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("d", "date", true,
				"the start date of data, such as: 20130101");
		o.setArgName("date");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("h", "hour", true, "hour, such as: 01");
		o.setArgName("hour");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("k", "kind", true, "log kind, such as: vv");
		o.setArgName("kind");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("q", "queue", true,
				"mapred.job.queue.name, such as: default");
		o.setArgName("queue");
		o.setRequired(false);
		options.addOption(o);

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(JOB_NAME + " ", options, true);
			System.exit(-1);
		}
		return cmd;
	}
}

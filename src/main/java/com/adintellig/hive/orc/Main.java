package com.adintellig.hive.orc;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
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
	public static final String JOB_OUTPUT_TEMP_DIR_SUFFIX = "_temp";

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

		/* valid */
		int dateInt = Integer.parseInt(date);
		if (dateInt < 19700101 || dateInt > 21000101)
			throw new Exception("date format invalid: " + date);
		int hourInt = Integer.parseInt(hour);
		if (hourInt < 0 || hourInt > 24)
			throw new Exception("hour format invalid: " + hour);

		/* config */
		String prefix = genOutputFileNamePrefix(kind, date, hour);
		if (null != prefix && prefix.trim().length() > 0)
			conf.set("hive.orc.output.filename.prefix", prefix.trim());
		if (null != kind && kind.trim().length() > 0)
			conf.set("hive.orc.input.log.kind", kind.trim().toLowerCase());
		if (null != queue && queue.trim().length() > 0)
			conf.set("mapred.job.queue.name", queue.trim());
		
		conf.setLong("mapred.max.split.size", 1024 * 1024 * 512);
		conf.setLong("mapred.min.split.size", 1024 * 1024 * 256);

		/* JOB */
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
		if (output.endsWith("/"))
			output = output.substring(0, output.length() - 1);
		Path tempOutputPath = new Path(output + "_" + hour
				+ JOB_OUTPUT_TEMP_DIR_SUFFIX);
		if (fs.exists(tempOutputPath))
			fs.delete(tempOutputPath, true);
		OrcOutputFormat.setOutputPath(job, tempOutputPath);

		JobClient.runJob(job);

		/* Copy data to output */
		long st = System.currentTimeMillis();
		Path outputPath = new Path(output);
		System.out.println("[copy]: " + tempOutputPath.toString() + " -> "
				+ outputPath.toString());

		List<Path> list = new ArrayList<Path>();
		FileStatus[] contents = fs.listStatus(tempOutputPath);
		for (int i = 0; i < contents.length; i++) {
			if (!contents[i].isDir()) {
				list.add(contents[i].getPath());
			}
		}
		Path[] srcPaths = new Path[list.size()];
		srcPaths = list.toArray(srcPaths);
		FileUtil.copy(fs, srcPaths, fs, outputPath, true, true, conf);

		if (fs.exists(tempOutputPath))
			fs.delete(tempOutputPath, true);
		long en = System.currentTimeMillis();
		System.out.println("Time cost: " + (en - st) / 1000 + "s.");
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

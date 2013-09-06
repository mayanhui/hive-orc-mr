package com.adintellig.hive.orc.mapred;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.adintellig.hive.orc.convertor.Convertor;
import com.adintellig.hive.orc.convertor.ConvertorFactory;
import com.adintellig.hive.orc.model.ModelClassFactory;

public class ORCMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, NullWritable, Writable> {

	Convertor convertor;
	OrcSerde serde;
	Class<?> clazz;
	
	@Override
	public void configure(JobConf job) {
		serde = new OrcSerde();
		
		String logKind = job.get("hive.orc.input.log.kind");
		clazz = ModelClassFactory.createClass(logKind);
		convertor = ConvertorFactory.createConvertor(logKind);
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<NullWritable, Writable> output, Reporter reporter)
			throws IOException {
		String valueStr = value.toString();

		StructObjectInspector inspector;
		inspector = (StructObjectInspector) ObjectInspectorFactory
				.getReflectionObjectInspector(clazz,
						ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

		output.collect(NullWritable.get(),
				serde.serialize(convertor.convert(valueStr), inspector));
	}

}

package org.shirdrn.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hdfs.HDFSFileSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MinimalWordCountBasedSparkRunner {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		PipelineOptions options = PipelineOptionsFactory.create();
		// options.setRunner(DirectRunner.class);
		// options.setRunner(SparkRunner.class);
		// options.setAppName(MinimalWordCountBasedSparkRunner.class.getSimpleName());
		// options.setSparkMaster("spark://myserver:7077");
		// Create the Pipeline object with the options we defined above.
		Pipeline pipeline = Pipeline.create(options);

		pipeline.apply(HDFSFileSource.readFrom(
				"hdfs://myserver:9000/data/ds/beam.txt", 
				TextInputFormat.class, LongWritable.class, Text.class))
				.apply("ExtractWords", ParDo.of(new DoFn<KV<LongWritable, Text>, String>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						for (String word : c.element().getValue().toString().split("[\\s:\\,\\.\\-]+")) {
							if (!word.isEmpty()) {
								c.output(word);
							}
						}
					}
				}))

				.apply(Count.<String> perElement())

				.apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
					@Override
					public String apply(KV<String, Long> input) {
						return input.getKey() + ": " + input.getValue();
					}
				}))

				.apply(TextIO.Write.to("countwords"));
		// .apply(TextIO.Write.to(new
		// HDFSFileSink("hdfs://myserver:9000/data/ds/output",
		// TextOutputFormat.class).createWriteOperation(options)));

		// Run the pipeline.
		pipeline.run().waitUntilFinish();
	}
}

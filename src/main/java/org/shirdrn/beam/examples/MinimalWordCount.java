package org.shirdrn.beam.examples;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class MinimalWordCount {

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();
		options.setRunner(DirectRunner.class);

		Pipeline pipeline = Pipeline.create(options);

		pipeline.apply(TextIO.Read.from("C:\\Users\\yanjun\\Desktop\\apache_beam.txt"))
				.apply("ExtractWords", ParDo.of(new DoFn<String, String>() {

					@ProcessElement
					public void processElement(ProcessContext c) {
						for (String word : c.element().split("[\\s:\\,\\.\\-]+")) {
							if (!word.isEmpty()) {
								c.output(word);
							}
						}
					}
					
				}))
				.apply(Count.<String> perElement())
				.apply("ConcatResultKVs", MapElements.via(
						new SimpleFunction<KV<String, Long>, String>() {
							
					@Override
					public String apply(KV<String, Long> input) {
						return input.getKey() + ": " + input.getValue();
					}
					
				}))
				.apply(TextIO.Write.to("wordcount"));

		pipeline.run().waitUntilFinish();
	}
}

package org.shirdrn.beam.examples;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;

public class DistinctExample {

	public static void main(String[] args) throws Exception {
		
		 PipelineOptions options = PipelineOptionsFactory.create();
		 options.setRunner(DirectRunner.class);
		 
		 Pipeline pipeline = Pipeline.create(options);
		 pipeline.apply(TextIO.Read.from("C:\\Users\\yanjun\\Desktop\\MY_ID_FILE.txt"))
			 .apply(Distinct.<String> create())
			 .apply(TextIO.Write.to("deduped.txt"));
		 pipeline.run().waitUntilFinish();
		    
	}
}
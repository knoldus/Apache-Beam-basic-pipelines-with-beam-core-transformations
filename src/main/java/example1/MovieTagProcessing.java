package example1;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The type Apache beam text io.
 */
final class ApacheBeamTextIO {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApacheBeamTextIO.class);

    private static final String CSV_HEADER = "userId,movieId,tag,timeStamp";

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read-Lines", TextIO.read().from("src/main/resources/source/movie_tags.csv"))
                .apply("Filter-Header", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(tokens[0],  (tokens[2]));
                        }))
                .apply("count-aggregation", Count.perElement())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(typeCount -> typeCount.getKey() + "," + typeCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to("src/main/resources/sink/movie_tag")
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("userId,movie_tag,Count"));

        LOGGER.info("Executing pipeline");
        pipeline.run();
    }

    //TODO: keep it in another class not as inner class.
    private static class FilterHeaderFn extends DoFn<String, String> {
        private static final Logger LOGGER = LoggerFactory.getLogger(FilterHeaderFn.class);

        private final String header;

        /**
         * Instantiates a new Filter header fn.
         *
         * @param header the header
         */
        FilterHeaderFn(String header) {
            this.header = header;
        }

        /**
         * Process element.
         *
         * @param processContext the process context
         */
        @ProcessElement
        public void processElement(ProcessContext processContext) {
            String row = processContext.element();
            if (!row.isEmpty() && !row.contains(header))
                processContext.output(row);
            else
                LOGGER.info("Filtered out the header of the csv file: [{}]", row);

        }
    }
}
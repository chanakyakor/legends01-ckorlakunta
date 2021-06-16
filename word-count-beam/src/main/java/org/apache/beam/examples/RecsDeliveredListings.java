package org.apache.beam.examples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.beam.sdk.io.TextIO;

public class RecsDeliveredListings {

    public static Long dateRange(String dateStr) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = df.parse(dateStr);
        return date.getTime();
    }

    public static <T> T last(T[] array) {
        return array[array.length - 1];
    }

    public interface GetListingOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://hadoop-sandbox-dev-default-sodf9k/fbahena/RecSysDeliveredListingsFlattenedWholeDay/2021_06_05/part-*.snappy.parquet")
        String getInputFile();

        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);

        @Description("CSV delimiter")
        @Default.String(",")
        String getCsvDelimiter();

        void setCsvDelimiter(String value);
    }

    static void runJob(GetListingOptions options) throws IOException {

        Pipeline p = Pipeline.create(options);

        String LOCALPATH = "/Users/ckorlakunta/development/word-count-dataflow/word-count-beam/src/main/java/org/apache/beam/examples/fbahena_RecSysDeliveredListingsFlattened_2021-05-30_part-00000-cc2e0463-26fe-4ed1-bb09-368ce563f19f-c000.snappy.parquet";
        Schema schema = convert(new Path(LOCALPATH));

        PCollection<GenericRecord> records = p.apply("Read Features", ParquetIO.read(schema).from(options.getInputFile())).setCoder(AvroCoder.of(GenericRecord.class, schema));
        String delivered = "recommendations_module_delivered";

        PCollection<GenericRecord> recsDeliveredVisits = records.apply("Filter for delivery event type",
                Filter.by(new SerializableFunction<GenericRecord, Boolean>() {
                    public Boolean apply(GenericRecord visitLog) {
                        String eventType = visitLog.get("event_type").toString();
                        return eventType.equals(delivered);
                    }
                }));

        PCollection<String> table2 = recsDeliveredVisits.apply(
                ParDo.of(new makeNewRow()));
        table2.apply(TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();

        PCollection<String> table1 = recsDeliveredVisits.apply(
                ParDo.of(new makeNewRow1()));
        table1.apply(TextIO.write().to("gs://hadoop-sandbox-dev-default-sodf9k/ckorlakunta/recs_delivered_listings/output/recsys_delivered_listings/"));

        p.run().waitUntilFinish();
    }

    static class makeNewRow extends DoFn<GenericRecord, String> {
        @ProcessElement
        public void processElement(@Element GenericRecord Row01, OutputReceiver<String> out) throws ParseException {

            List<String> datasets = new ArrayList<>();
            List<String> listing_ids = new ArrayList<>();
            if (Row01.get("datasets").toString() != "[]") {
                datasets = Arrays.asList(Row01.get("datasets").toString().replaceAll("[\\[\\]\"]", "").split(","));
            }
            if (Row01.get("listing_ids") != "") {
                listing_ids = Arrays.asList(Row01.get("listing_ids").toString().split(","));
            }
            //"run_date,visit_id,1,module_placement"
            ListIterator<String> listing_id = listing_ids.listIterator();
            while (listing_id.hasNext()) {

                StringBuilder output = new StringBuilder("");
                String visit_id = Row01.get("visit_id").toString();
                String module_placement = Row01.get("module_placement").toString();
                int run_date = dateRange("2021-06-18 00:00:00.000").intValue() / 1000;
                output.append(run_date);
                output.append(",");
                output.append(visit_id);
                output.append(",");
                output.append("1,");
                output.append(module_placement);
                output.append(",");
                if (datasets.size() == 0) {
                    output.append("null");
                } else {
                    output.append(datasets.get(0));
                }
                output.append(",");
                output.append(datasets.subList(1, datasets.size()).stream().map(Object::toString).collect(Collectors.joining(",")));
                output.append(",");
                output.append(listing_id.nextIndex() + 1);
                output.append(",");
                output.append(listing_id.next());
                output.append(",");
                output.append("1");
                out.output(output.toString());
            }
        }
    }

    static class makeNewRow1 extends DoFn<GenericRecord, String> {
        @ProcessElement
        public void processElement(@Element GenericRecord Row01, OutputReceiver<String> out) throws ParseException {

            StringBuilder output = new StringBuilder("");
            String visit_id = Row01.get("visit_id").toString();
            String module_placement = Row01.get("module_placement").toString();
            int run_date = dateRange("2021-06-18 00:00:00.000").intValue() / 1000;
            //"run_date,visit_id,1,module_placement"
            output.append(run_date);
            output.append(",");
            output.append(visit_id);
            output.append(",");
            output.append("1,");
            output.append(module_placement);
            out.output(output.toString());
        }
    }

    public static Schema convert(Path parquetPath) throws IOException {

        Configuration cfg = new Configuration();
        cfg.set("parquet.strict.typing", "true");
        cfg.set("parquet.avro.add-list-element-records", "true");
        cfg.set("parquet.avro.write-old-list-structure", "true");

        //cfg.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        //cfg.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

        // Create parquet reader
        ParquetFileReader rdr = ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, cfg));
        // Get parquet schema
        MessageType schema = rdr.getFooter().getFileMetaData().getSchema();
        // Convert to Avro
        Schema avroSchema = new AvroSchemaConverter(cfg).convert(schema);
        rdr.close();
        return avroSchema;
    }

    public static void main(String[] args) throws IOException {
        GetListingOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(GetListingOptions.class);

        runJob(options);
    }
}

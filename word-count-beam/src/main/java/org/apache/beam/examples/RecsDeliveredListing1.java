package org.apache.beam.examples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.TextIO;


public class RecsDeliveredListing1 {

public static Long dateRange(String dateStr) throws ParseException {
SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
Date date = df.parse(dateStr);
return date.getTime();
}

public static <T> T last(T[] array) {
return array[array.length - 1];
}

public static void main(String[] args) throws IOException, ParseException {


    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    String GCSPATH = "gs://hadoop-sandbox-dev-default-sodf9k/fbahena/RecSysDeliveredListingsFlattenedWholeDay/2021_06_05/part-*.snappy.parquet";
//    String LOCALPATH = "/Users/ckorlakunta/development/word-count-dataflow/word-count-beam/src/main/java/org/apache/beam/examples/Flattened_2021-05-30_part-00000-cc2e0463-26fe-4ed1-bb09-368ce563f19f-c000.snappy.parquet";
    String OUTPUTPATH = "gs://hadoop-sandbox-dev-default-sodf9k/ckorlakunta/recs_delivered_listings/dataflow_output1";
    Schema AVSCHEMA = convert(GCSPATH);
    System.out.println(AVSCHEMA);

    PCollection<GenericRecord> records = p.apply(ParquetIO.read(AVSCHEMA).from(GCSPATH));
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
    table2.apply(TextIO.write().to(OUTPUTPATH));

    p.run().waitUntilFinish();
}

    static class makeNewRow extends DoFn<GenericRecord, String> {
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


//    {"type":"record","name":"spark_schema",
//"fields":[{"name":"visit_id","type":["null","string"],"default":null},
//        {"name":"event_type","type":["null","string"],"default":null},
//        {"name":"listing_ids","type":["null","string"],"default":null},
//        {"name":"module_placement","type":["null","string"],"default":null},
//        {"name":"datasets","type":["null","string"],"default":null}]}
//{"type":"record","name":"ParquetSchema",
//        "fields":[{"name":"visit_id","type":"string"},
//    {"name":"browser_id","type":["null","string"],"default":null},
//    {"name":"start_epoch_ms","type":"long"},
//    {"name":"end_epoch_ms","type":"long"},
//    {"name":"primary_count","type":"int"},
//    {"name":"event_source","type":["null","string"],"default":null},
//    {"name":"is_bounce","type":"boolean"},

    public static Schema convert (String parquetPath) throws IOException {

        Configuration cfg = new Configuration();
        cfg.set("parquet.strict.typing", "true");
        cfg.set("parquet.avro.add-list-element-records", "true");
        cfg.set("parquet.avro.write-old-list-structure", "true");

        // Create parquet reader
        ParquetFileReader rdr = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(parquetPath), cfg));
        // Get parquet schema
        MessageType schema = rdr.getFooter().getFileMetaData().getSchema();
        // Convert to Avro
        Schema avroSchema = new AvroSchemaConverter(cfg).convert(schema);
        rdr.close();
        return avroSchema;
    }
}

package org.apache.beam.examples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class RecsysDeliveredListings {

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

    String GCSPATH = "gs://etldata-prod-adhoc-data-hkwv8r/user/sbaskaran/temp/data_flow/recsDeliveredVisits/visit_log_exploded_sample/part-00000-156e3cba-5d79-47e0-b7b8-af5aa9327f0b-c000.snappy.parquet";
    String LOCALPATH = "/Users/ckorlakunta/development/word-count-dataflow/word-count-beam/src/main/java/org/apache/beam/examples/fbahena_RecSysDeliveredListingsFlattened_2021-05-30_part-00000-cc2e0463-26fe-4ed1-bb09-368ce563f19f-c000.snappy.parquet";

    Schema AVSCHEMA = convert(LOCALPATH);
    System.out.println(AVSCHEMA);


    PCollection<GenericRecord> records = p.apply(ParquetIO.read(AVSCHEMA).from(GCSPATH));
    String delivered = "search2_organic_listings_group_seen";
    Integer runDate = dateRange("2021-06-18 00:00:00.000").intValue() / 1000;

    PCollection<GenericRecord> recsDeliveredVisits = records.apply("Filter for delivery event type",
            Filter.by(new SerializableFunction<GenericRecord, Boolean>() {
              public Boolean apply(GenericRecord visitLog) {
                GenericRecord event = (GenericRecord) visitLog.get("events");
                String eventType = event.get("event_type").toString();
                Integer numEvents = Integer.parseInt(event.get("num_events").toString());
                return numEvents < 1000 && eventType.equals(delivered) ;
              }
            }));

    // module level table
    // get delivered event for all modules

//        recsDeliveredVisits
//                .flatMapTo(('visit, 'visit_id) -> ('run_date, 'visit_id, 'sequence_number, 'module_name)) {
//            (v: Visit, visitId: String) =>
//
//            v.collection.zipWithIndex
//                    .filter { case (e, i) => e.eventType == delivered }
//          .map { case (e, i) =>
//                (runDate,
//                        visitId,
//                        i,
//                        e.prop("module_placement"))
//            }
//        }
//
    PCollection<GenericRecord> table2 = recsDeliveredVisits.apply("Get delivered event for all modules",
            ParDo.of(new DoFn<GenericRecord, GenericRecord>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                GenericRecord visitLog = c.element();
                Integer visitId = (Integer) visitLog.get("visit_id");
                Integer seq = (Integer) visitLog.get("seq");
                Integer modulePlacement = (Integer) visitLog.get("module_placement");
              }
            }));


    recsDeliveredVisits.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(AVSCHEMA)).to("output/visit_log_exploded_sample/"));
    p.run().waitUntilFinish();
  }

  public static Schema convert(String parquetPath) throws IOException {

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

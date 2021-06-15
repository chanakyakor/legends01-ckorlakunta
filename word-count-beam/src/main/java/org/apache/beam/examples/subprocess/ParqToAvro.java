package org.apache.beam.examples.subprocess;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;


public class ParqToAvro {

    public static void main(String[] args) throws IOException {


        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        String LOCALPATH = "/Users/ckorlakunta/development/part-00000-db674e6c-aaf5-4e15-8262-c80d073f30d4-c000.snappy.parquet";
        Schema AVSCHEMA = convert(new Path(LOCALPATH));
        System.out.println(AVSCHEMA);

        String GCSPATH = "gs://etldata-prod-adhoc-data-hkwv8r/user/sbaskaran/temp/data_flow/visit_log_exploded/part-00000-db674e6c-aaf5-4e15-8262-c80d073f30d4-c000.snappy.parquet";

        PCollection<GenericRecord> records = p.apply(ParquetIO.read(AVSCHEMA).from(GCSPATH));
        records.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(AVSCHEMA)).to("output/visit_log_sample2/"));
        p.run().waitUntilFinish();

    }

    public static Schema convert(Path parquetPath) throws IOException {
        Configuration cfg = new Configuration();
        // Create parquet reader
        ParquetFileReader rdr = ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, cfg));
        // Get parquet schema
        MessageType schema = rdr.getFooter().getFileMetaData().getSchema();
        // Convert to Avro
        Schema avroSchema = new AvroSchemaConverter(cfg).convert(schema);
        rdr.close();
        return avroSchema;
    }

}

package com.google.cloud.teleport.v2.mongodb.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptDocumentTransformer;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import java.io.IOException;
import javax.script.ScriptException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Template(
        name = "MongoDB_to_BigQuery_CDC_Custom",
        category = TemplateCategory.STREAMING,
        displayName = "MongoDB to BigQuery (CDC) Custom",
        description =
                "The MongoDB to BigQuery CDC (Change Data Capture) template is a streaming pipeline that works together with MongoDB change streams. "
                        + "The pipeline reads the JSON records pushed to Pub/Sub via a MongoDB change stream and writes them to BigQuery.",
        optionsClass = MongoDbToBigQueryCdc.Options.class,
        flexContainerName = "mongodb-to-bigquery-cdc",
        documentation =
                "https://cloud.google.com/dataflow/docs/guides/templates/provided/mongodb-change-stream-to-bigquery",
        contactInformation = "https://cloud.google.com/support",
        preview = true,
        requirements = {
                "The target BigQuery dataset must exist.",
                "The change stream pushing changes from MongoDB to Pub/Sub should be running."
        },
        streaming = true,
        supportsAtLeastOnce = true)
public class MongoDbToBigQueryCdcCustom {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDbToBigQuery.class);

    /** Options interface. */
    public interface Options
            extends PipelineOptions,
            MongoDbToBigQueryOptions.MongoDbOptions,
            MongoDbToBigQueryOptions.PubSubOptions,
            MongoDbToBigQueryOptions.BigQueryWriteOptions,
            MongoDbToBigQueryOptions.JavascriptDocumentTransformerOptions,
            BigQueryStorageApiStreamingOptions {

        // Hide the UseStorageWriteApiAtLeastOnce in the UI, because it will automatically be turned
        // on when pipeline is running on ALO mode and using the Storage Write API
        @TemplateParameter.Boolean(
                order = 1,
                optional = true,
                parentName = "useStorageWriteApi",
                parentTriggerValues = {"true"},
                description = "Use at at-least-once semantics in BigQuery Storage Write API",
                helpText =
                        "When using the Storage Write API, specifies the write semantics. To"
                                + " use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-"
                                + " once semantics, set the parameter to `false`. This parameter applies only when"
                                + " `useStorageWriteApi` is `true`. The default value is `false`.",
                hiddenUi = true)
        @Default.Boolean(false)
        @Override
        Boolean getUseStorageWriteApiAtLeastOnce();

        void setUseStorageWriteApiAtLeastOnce(Boolean value);
    }

    /** class ParseAsDocumentsFn. */
    private static class ParseAsDocumentsFn extends DoFn<String, Document> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            context.output(Document.parse(context.element()));
        }
    }

    /**
     * Main entry point for pipeline execution.
     *
     * @param args Command line arguments to the pipeline.
     */
    public static void main(String[] args)
            throws ScriptException, IOException, NoSuchMethodException {
        UncaughtExceptionLogger.register();

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
        run(options);
    }

    /** Pipeline to read data from PubSub and write to MongoDB. */
    public static boolean run(Options options)
            throws ScriptException, IOException, NoSuchMethodException {
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        String inputOption = options.getInputTopic();
        String jsTransformPath =
                String.format("gs://hevo_bookipi_dev/%s/transforms/transform.js", options.getCollection());
        String jsTransformFunctionName = "process";

        pipeline
                .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(inputOption))
                .apply(
                        "RTransform string to document",
                        ParDo.of(
                                new DoFn<String, Document>() {
                                    @ProcessElement
                                    public void process(ProcessContext c) {
                                        Document document = Document.parse(c.element());
                                        c.output(document);
                                    }
                                }))
                .apply(
                        "UDF",
                        JavascriptDocumentTransformer.TransformDocumentViaJavascript.newBuilder()
                                .setFileSystemPath(jsTransformPath)
                                .setFunctionName(jsTransformFunctionName)
                                .build())
                .apply(
                        "Read and transform data",
                        ParDo.of(
                                new DoFn<Document, TableRow>() {
                                    @ProcessElement
                                    public void process(ProcessContext c) {
                                        Document document = c.element();
                                        TableRow row = BigQueryUtils.getTableSchema(document);
                                        c.output(row);
                                    }
                                }))
                .apply(
                        BigQueryIO.writeTableRows()
                                .to(options.getOutputTableSpec())
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        pipeline.run();
        return true;
    }
}


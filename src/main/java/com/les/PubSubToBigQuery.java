/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.les;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * To run locally
 * ./gradlew clean execute -DmainClass=com.les.PubSubToBigQuery  -Dexec.args="--project=gcp-bigdata-313810 --inputSubscription=projects/gcp-bigdata-313810/subscriptions/order-events-dataflow-subscription --AOutputTableSpec=gcp-bigdata-313810:order_events_dataset.a_names --BOutputTableSpec=gcp-bigdata-313810:order_events_dataset.b_names  --runner=DirectRunner --region=us-central1"
 *
 * To create dataflow job on GCP
 * ./gradlew clean execute -DmainClass=com.les.PubSubToBigQuery  -Dexec.args="--project=gcp-bigdata-313810 --inputSubscription=projects/gcp-bigdata-313810/subscriptions/order-events-dataflow-subscription --AOutputTableSpec=gcp-bigdata-313810:order_events_dataset.a_names --BOutputTableSpec=gcp-bigdata-313810:order_events_dataset.b_names  --runner=DataflowRunner --region=us-central1"
 */
public class PubSubToBigQuery {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQuery.class);

    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions {
        @Description("Table spec to write the output to")
        ValueProvider<String> getAOutputTableSpec();

        void setAOutputTableSpec(ValueProvider<String> value);

        @Description("Table spec to write the output to")
        ValueProvider<String> getBOutputTableSpec();

        void setBOutputTableSpec(ValueProvider<String> value);

        @Description(
                "The Cloud Pub/Sub subscription to consume from. "
                        + "The name should be in the format of "
                        + "projects/<project-id>/subscriptions/<subscription-name>.")
        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * PubSubToBigQuery#run(Options)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {

        Pipeline pipeline = Pipeline.create(options);


        final TupleTag<String> startsWithATag = new TupleTag<String>(){};
        final TupleTag<String> startsWithBTag = new TupleTag<String>(){};

        /*
         * Step #1: Read messages in from Pub/Sub Subscription
         */
        PCollectionTuple mixedCollection = pipeline
                .apply(
                        "Read from pubsub subscription",
                        PubsubIO.readMessagesWithAttributes()
                                .fromSubscription(options.getInputSubscription())
                )

                /*
                 * Step #2: Extract names from 'Hello <name>!' message
                 */
                .apply("Extract names", MapElements.via(new SimpleFunction<PubsubMessage, String>() {
                    @Override
                    public String apply(PubsubMessage message) {
                        String payload = new String(message.getPayload());

                        String name = payload.split(" ")[1];
                        return name.substring(0, name.length() - 1);
                    }
                }))
                /*
                 * Step #3: Filter messages by length
                 */
                .apply("Reject names with length > 5", Filter.by((ProcessFunction<String, Boolean>) name -> name.length() < 5))
                /*
                 * Step #4: Split names by first letter
                 */
                .apply("Split output", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        if (c.element().startsWith("A")) {
                            // Emit to main output, which is the output with tag startsWithATag.
                            c.output(startsWithATag, c.element());
                        } else if (c.element().startsWith("B")) {
                            // Emit to output with tag startsWithBTag.
                            c.output(startsWithBTag, c.element());
                        }
                    }
                }).withOutputTags(startsWithATag, TupleTagList.of(startsWithBTag)));

        /*
         * Step #5: Convert 'A' names to TableRAW and write to BigQuery
         */
        mixedCollection.get(startsWithATag)
                .apply("Convert A to TableRow", new NameToTableRowTransform())
                .apply("Write A to BigQuery", getOutput(options.getAOutputTableSpec()));

        /*
         * Step #5: Convert 'B' names to TableRAW and write to BigQuery
         */
        mixedCollection.get(startsWithBTag)
                .apply("Convert B to TableRow", new NameToTableRowTransform())
                .apply("Write B to BigQuery", getOutput(options.getBOutputTableSpec()));

        return pipeline.run();

    }

    private static BigQueryIO.Write<TableRow> getOutput(ValueProvider<String> outputProvider) {
        return BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .to(outputProvider);
    }


    static class NameToTableRowTransform
            extends PTransform<PCollection<String>, PCollection<TableRow>> {

        @Override
        public PCollection<TableRow> expand(PCollection<String> input) {
            return
                    input
                            .apply(
                                    "Map name to table row",
                                    ParDo.of(new DoFn<String, TableRow>() {
                                        @ProcessElement
                                        public void processElement(ProcessContext context) {
                                            String name = context.element();

                                            TableRow tableRow = new TableRow()
                                                    .set("name", name)
                                                    .set("timestamp", Instant.now().toString());

                                            LOG.info("Row: {}", tableRow);

                                            context.output(tableRow);
                                        }
                                    })
                            );
        }
    }

}
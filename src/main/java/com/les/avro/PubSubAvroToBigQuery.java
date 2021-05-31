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

package com.les.avro;

import com.google.api.services.bigquery.model.TableRow;
import com.les.model.UserMessage;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.time.Instant;

/**
 * To run locally
 * ./gradlew clean execute -DmainClass=com.les.avro.PubSubAvroToBigQuery  -Dexec.args="--project=gcp-bigdata-313810 --inputTopic=projects/gcp-bigdata-313810/topics/user-messages-avro --outputTableSpec=gcp-bigdata-313810:order_events_dataset.user_messages  --runner=DirectRunner --region=us-central1"
 * <p>
 * To create dataflow job on GCP
 * ./gradlew clean execute -DmainClass=com.les.avro.PubSubAvroToBigQuery  -Dexec.args="--project=gcp-bigdata-313810 --inputTopic=projects/gcp-bigdata-313810/topics/user-messages-avro --outputTableSpec=gcp-bigdata-313810:order_events_dataset.user_messages  --runner=DataflowRunner --region=us-central1"
 */
public class PubSubAvroToBigQuery {

    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions {
        @Description("Table spec to write the output to")
        ValueProvider<String> getOutputTableSpec();

        void setOutputTableSpec(ValueProvider<String> value);

        @Description(
                "The Cloud Pub/Sub topic to consume from.")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * PubSubAvroToBigQuery#run(Options)} method to start the pipeline and invoke {@code
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

        // Prepare avro reader

        /*
         * Step #1: Read messages in from Pub/Sub Subscription
         */
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(
                        "Read from pubsub subscription",
                        PubsubIO.readAvros(UserMessage.class)
                                .fromTopic(options.getInputTopic())
                )
                .apply(
                        "Transform UserMessage to TableRow",
                        MapElements.via(new SimpleFunction<UserMessage, TableRow>() {
                            @Override
                            public TableRow apply(UserMessage userMessage) {
                                return new TableRow()
                                        .set("id", userMessage.getId())
                                        .set("first_name", userMessage.getFirstName())
                                        .set("last_name", userMessage.getLastName())
                                        .set("timestamp", Instant.now().toString());
                            }
                        })
                )
                .apply(
                        "Write to BigQuery",
                        BigQueryIO.writeTableRows()
                                .withoutValidation()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .to(options.getOutputTableSpec())
                );

        return pipeline.run();
    }
}
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
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static com.les.model.UserMessageOuterClass.UserMessage;

/**
 * To run locally
 * ./gradlew clean execute -DmainClass=com.les.protobuf.PubSubProtobufToBigQuery  -Dexec.args="--project=gcp-bigdata-313810 --inputSubscription=projects/gcp-bigdata-313810/subscriptions/user-messages-sub --outputTableSpec=gcp-bigdata-313810:order_events_dataset.user_messages  --runner=DirectRunner --region=us-central1"
 * <p>
 * To create dataflow job on GCP
 * ./gradlew clean execute -DmainClass=com.les.protobuf.PubSubProtobufToBigQuery  -Dexec.args="--project=gcp-bigdata-313810 --inputSubscription=projects/gcp-bigdata-313810/subscriptions/user-messages-sub --outputTableSpec=gcp-bigdata-313810:order_events_dataset.user_messages  --runner=DataflowRunner --region=us-central1"
 */
public class PubSubProtobufToBigQuery {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubProtobufToBigQuery.class);

    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions {
        @Description("Table spec to write the output to")
        ValueProvider<String> getOutputTableSpec();

        void setOutputTableSpec(ValueProvider<String> value);

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
     * PubSubProtobufToBigQuery#run(Options)} method to start the pipeline and invoke {@code
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

        /*
         * Step #1: Read messages in from Pub/Sub Subscription
         */
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(
                        "Read from pubsub subscription",
                        PubsubIO.readMessagesWithAttributes()
                                .fromSubscription(options.getInputSubscription())
                )
                .apply(
                        "Transform PubsubMessage to UserMessage",
                        MapElements.via(new SimpleFunction<PubsubMessage, UserMessage>() {
                            @Override
                            public UserMessage apply(PubsubMessage pubsubMessage) {
                                try {
                                    return UserMessage.parseFrom(pubsubMessage.getPayload());
                                } catch (InvalidProtocolBufferException e) {
                                    LOG.error("{}", e.getMessage());
                                    throw new RuntimeException(e);
                                }
                            }
                        })
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
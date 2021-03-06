package com.les.avro;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.les.model.UserMessage;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class PubSubAvroExample {

    public static void main(String[] args) throws Exception {
        String projectId = System.getenv("GCP_EXAMPLES_PROJECT_ID");
        String topicId = System.getenv("GCP_EXAMPLES_TOPIC_ID");
        String pathToCredentials = System.getenv("GCP_CREDENTIALS");

        TopicName topic = TopicName.ofProjectTopicName(projectId, topicId);

        Publisher publisher = null;
        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topic).setCredentialsProvider(new FixedCredentialsProvider() {
                @Nullable
                @Override
                public Credentials getCredentials() {
                    try {
                        return GoogleCredentials.fromStream(new FileInputStream(pathToCredentials));
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }).build();

            UserMessage userMessage = UserMessage.newBuilder()
                    .setId(UUID.randomUUID().toString())
                    .setFirstName("Avro")
                    .setLastName("Example")
                    .build();

            // Encode the object and write it to the output stream.
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(byteArrayOutputStream, null);
            userMessage.customEncode(encoder);
            encoder.flush();

            // Create pubsub message
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(ByteString.copyFrom(byteArrayOutputStream.toByteArray()))
                    .build();

            // Publish
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);

            // Once published, returns a server-assigned message id (unique within the topic)
            String messageId = messageIdFuture.get();
            System.out.println("Published message ID: " + messageId);
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }
}

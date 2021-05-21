package com.les;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class PubSubExample {

    public static void main(String[] args) throws Exception {
        String projectId = "<project-id>";
        String topicId = "<topic-id>";
        String pathToCredentials = "<path-to-credentials>";

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

            String[] names = {"A", "B", "AA", "BB", "AAAAA", "BBBBB"};

            for (String name: names) {
                Thread.sleep(5000);

                String message = "Hello " + name +  "!";

                // Create pubsub message
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Publish
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);

                // Once published, returns a server-assigned message id (unique within the topic)
                String messageId = messageIdFuture.get();
                System.out.println("Published message ID: " + messageId);
            }
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }
}

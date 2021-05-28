package com.les;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;

import java.io.FileInputStream;
import java.time.Instant;
import java.util.Map;


public class BigQueryExample {

    public static void main(String[] args) throws Exception {
        String projectId = System.getenv("GCP_EXAMPLES_PROJECT_ID");
        String datasetId = System.getenv("GCP_EXAMPLES_DATASET_ID");
        String table = System.getenv("GCP_EXAMPLES_TABLE_ID");
        String pathToCredentials = System.getenv("GCP_CREDENTIALS");

        // Create a service instance
        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setProjectId(projectId)
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream(pathToCredentials)))
                .build()
                .getService();

        TableId tableId = TableId.of(datasetId, table);

//         Define rows to insert
        Map<String, Object> firstRow = Map.of("name", "Name1", "timestamp", Instant.now().toString());
        Map<String, Object> secondRow = Map.of("name", "Name2", "timestamp", Instant.now().toString());

        InsertAllRequest insertRequest =
                InsertAllRequest.newBuilder(tableId).addRow(firstRow).addRow(secondRow).build();
//         Insert rows
        InsertAllResponse insertResponse = bigquery.insertAll(insertRequest);
//         Check if errors occurred
        if (insertResponse.hasErrors()) {
            System.out.println("Errors occurred while inserting rows");
        }

        System.out.println(insertResponse);

        // Create a query request
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder("SELECT * FROM " + datasetId + "." + table).build();

        // Read rows
        System.out.println("Table rows:");
        for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
            System.out.println(row);
        }
    }
}

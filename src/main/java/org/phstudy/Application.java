package org.phstudy;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.zetasketch.HyperLogLogPlusPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.UUID;

public class Application {
    private final static String GOOGLE_APPLICATION_CREDENTIALS_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS";
    private final static String GOOGLE_APPLICATION_CREDENTIALS_PATH = "/path/to/my/key.json";

    private static final Logger logger = LoggerFactory.getLogger(Application.class);


    public static void main(String[] args) throws InterruptedException, IOException {
        HyperLogLogPlusPlus<String> hll1 = genHllFromZetaSketch();
        HyperLogLogPlusPlus<String> hll2 = genHllFromBigQuery();

        long cardinality1 = hll1.longResult();
        long cardinality2 = hll2.longResult();
        long cardinality3 = calcZetaSketchHLLCardinalityInBigQuery(hll1);

        logger.info("ZetaSketch Cardinality: {}", cardinality1);
        logger.info("ZetaSketch Cardinality (from BigQuery HLL_COUNT): {}", cardinality2);
        logger.info("BigQuery HLL_COUNT Cardinality (from ZetaSketch HLL): {}", cardinality3);
    }

    public static HyperLogLogPlusPlus<String> genHllFromZetaSketch() {
        HyperLogLogPlusPlus<String> hll = new HyperLogLogPlusPlus.Builder().buildForStrings();

        hll.add("apple");
        hll.add("orange");
        hll.add("banana");

        return hll;
    }

    public static HyperLogLogPlusPlus<String> genHllFromBigQuery() throws InterruptedException, IOException {
        String sql = //
                  "SELECT"
                + "  HLL_COUNT.INIT(fruit) AS fruit_hll"
                + " FROM UNNEST(['apple', 'orange', 'banana']) AS fruit";

        TableResult result = queryBigQuery(sql);

        // Return hll.
        FieldValueList row = result.getValues().iterator().next();
        byte[] hllBytes = row.get("fruit_hll").getBytesValue();
        HyperLogLogPlusPlus<String> rst = (HyperLogLogPlusPlus<String>) HyperLogLogPlusPlus.forProto(hllBytes);

        return rst;
    }

    public static long calcZetaSketchHLLCardinalityInBigQuery(HyperLogLogPlusPlus<String> hll) throws
                                                                                               InterruptedException,
                                                                                               IOException {
        String base64Str = Base64.getEncoder().encodeToString(hll.serializeToByteArray());
        String sql = "SELECT HLL_COUNT.EXTRACT(FROM_BASE64('" + base64Str + "')) AS fruit_cnt";

        TableResult result = queryBigQuery(sql);

        // Return hll.
        FieldValueList row = result.getValues().iterator().next();
        long cnt = row.get("fruit_cnt").getLongValue();

        return cnt;
    }

    public static TableResult queryBigQuery(String sql) throws InterruptedException, IOException {
        BigQuery bigquery;

        if (System.getenv().containsKey(GOOGLE_APPLICATION_CREDENTIALS_ENV_VAR)) {
            bigquery = BigQueryOptions.getDefaultInstance().getService();
        } else {
            bigquery = BigQueryOptions.newBuilder()
                                      .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(GOOGLE_APPLICATION_CREDENTIALS_PATH)))
                                      .build()
                                      .getService();
        }

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(sql)
                                     // Use standard SQL syntax for queries.
                                     // See: https://cloud.google.com/bigquery/sql-reference/
                                     .setUseLegacySql(false)
                                     .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        return queryJob.getQueryResults();
    }
}

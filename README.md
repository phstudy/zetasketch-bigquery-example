Example ZetaSketch with BigQuery
=================================

An Example demonstrates how to use [ZetaSketch](https://github.com/google/zetasketch) with [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions).

# How to run
```bash
$ git clone https://github.com/phstudy/zetasketch-bigquery-example.git
$ cd zetasketch-bigquery-example
$ GOOGLE_APPLICATION_CREDENTIALS=/path/to/my/key.json ./gradlew run
```

# Sample code
### Generate ZetaSketch HLL

```java
HyperLogLogPlusPlus<String> hll = new HyperLogLogPlusPlus.Builder().buildForStrings();

hll.add("apple");
hll.add("orange");
hll.add("banana");    
```    

### Generate HLL in BigQuery and deserialize as ZetaSketch HLL object
```java
String sql = //
          "SELECT"
        + "  HLL_COUNT.INIT(fruit) AS fruit_hll"
        + " FROM UNNEST(['apple', 'orange', 'banana']) AS fruit";

TableResult result = queryBigQuery(sql);

FieldValueList row = result.getValues().iterator().next();
byte[] hllBytes = row.get("fruit_hll").getBytesValue();
HyperLogLogPlusPlus<String> rst = (HyperLogLogPlusPlus<String>) HyperLogLogPlusPlus.forProto(hllBytes);
```    
### Serialize ZetaSketch HLL object and calculate cardinality in BigQuery
```java
String base64Str = Base64.getEncoder().encodeToString(hll.serializeToByteArray());
String sql = "SELECT HLL_COUNT.EXTRACT(FROM_BASE64('" + base64Str + "')) AS fruit_cnt";

TableResult result = queryBigQuery(sql);

FieldValueList row = result.getValues().iterator().next();
long cnt = row.get("fruit_cnt").getLongValue();
```
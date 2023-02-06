package dev.kunal;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;

import java.util.Map;
import java.io.InputStream;

public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Main.class.getName());
        JavaSparkContext sc = null;
        try {
            sc = new JavaSparkContext(conf);

            SparkSession spark = SparkSession
                    .builder()
                    .appName(Main.class.getName())
                    .getOrCreate();

            InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("application.yaml");
            Map<String, Object> map = new Yaml().load(inputStream);
            String bucketName = (String) map.get("spring.cloud.aws.s3.bucket");
            String csvFilePath = (String) map.get("spring.cloud.aws.s3.bucket-path");
            String s3Uri = "s3://" + bucketName + "/" + csvFilePath + "/*.csv";
            log.info("Reading the CSV files from S3: " + s3Uri);


//            JavaRDD<String> rdd = sc.textFile(s3Uri);
            JavaRDD<String> rdd = sc.textFile("src/main/resources/sample.csv");

            Dataset<Row> df = spark.createDataFrame(rdd, String.class);
            df.createOrReplaceTempView("temp_table");

            Dataset<Row> result = spark.sql("SELECT * FROM temp_table");
            result.show();
        } catch (Exception e) {
            log.error("Error reading the CSV file from S3: " + e.getMessage());
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }
}
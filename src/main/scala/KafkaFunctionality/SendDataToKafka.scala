package KafkaFunctionality

import okhttp3.{OkHttpClient, Request}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object SendDataToKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate()
    while (true) {
      val client = new OkHttpClient()

      println("60")
      val city = "london"
      val endpoint = buildEndpoint1(city)
      val request = new Request.Builder()
        .url(s"https://weatherapi-com.p.rapidapi.com/$endpoint")
        .get()
        .addHeader("X-RapidAPI-Key", "2467bc2bd9mshfd02cfd8d4e5a77p13ba22jsn5eecabaf967e")
        .addHeader("X-RapidAPI-Host", "weatherapi-com.p.rapidapi.com")
        .build();


      val response = client.newCall(request).execute();
      val responseBody = response.body().string()
      print(responseBody)


      val schema = StructType(
        Array(
          StructField("location", StructType(
            Array(
              StructField("name", StringType),
              StructField("region", StringType),
              StructField("country", StringType),
              StructField("lat", DoubleType),
              StructField("lon", DoubleType),
              StructField("tz_id", StringType),
              StructField("localtime_epoch", LongType),
              StructField("localtime", StringType)
            )
          )),
          StructField("current", StructType(
            Array(
              StructField("last_updated_epoch", LongType),
              StructField("last_updated", StringType),
              StructField("temp_c", DoubleType),
              StructField("temp_f", DoubleType),
              StructField("is_day", IntegerType),
              StructField("condition", StructType(
                Array(
                  StructField("text", StringType),
                  StructField("icon", StringType),
                  StructField("code", IntegerType)
                )
              )),
              StructField("wind_mph", DoubleType),
              StructField("wind_kph", DoubleType),
              StructField("wind_degree", IntegerType),
              StructField("wind_dir", StringType),
              StructField("pressure_mb", DoubleType),
              StructField("pressure_in", DoubleType),
              StructField("precip_mm", DoubleType),
              StructField("precip_in", DoubleType),
              StructField("humidity", IntegerType), // Extracting humidity
              StructField("cloud", IntegerType),
              StructField("feelslike_c", DoubleType),
              StructField("feelslike_f", DoubleType),
              StructField("vis_km", DoubleType),
              StructField("vis_miles", DoubleType),
              StructField("uv", DoubleType),
              StructField("gust_mph", DoubleType),
              StructField("gust_kph", DoubleType)
            )
          ))
        )
      )
      import spark.implicits._
      // Read JSON string as DataFrame using the defined schema
      val dfFromText: DataFrame = spark.read.schema(schema).json(Seq(responseBody).toDS)
      println(dfFromText)
      // Set the time zone to Europe/London
      val londonZoneId = ZoneId.of("Europe/London")
      // Fetch the London local time
      val londonLocalTime = LocalDateTime.now(londonZoneId).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      // Selecting required fields and extracting wind and humidity
      val extractedDF = dfFromText.select(
        $"current.wind_mph".alias("wind_mph"),
        lit(londonLocalTime).alias("localtime")
      )

      // Assuming you want to create a messageDF with specific columns and pass wind and humidity
      val messageDF = extractedDF.select(
        $"wind_mph",$"localtime")


      val kafkaServer: String = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
      val topicSampleName: String = "y_w_f"
      println("63")
      messageDF.show()
      // Assuming messageDF contains 'wind_mph' and 'humidity' columns
      val formattedDF = messageDF.withColumn("value", to_json(struct($"wind_mph",$"localtime")))

      formattedDF
        .selectExpr("CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServer)
        .option("topic", topicSampleName)
        .save()

      println("Message is loaded to Kafka topic!!")
      Thread.sleep(60000) // Wait for 10 seconds before making the next call

    }
  }

  def buildEndpoint1(city: String): String = {
    s"current.json?q=$city"
  }

}
package com.ramesh.weather.spark.helper;

import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.*;


//We can have constants for all these repeated columns
@UtilityClass
public class WeatherStationServiceHelper {
    /**
     * Rank stations by how long they have online
     *
     * @param weatherStationsData
     */
    public void rankStationsByOnline(final Dataset<Row> weatherStationsData) {
        Dataset<Row> df1 = weatherStationsData.groupBy("station").agg(count("station").alias("totalMeasures")).select("*");
        df1.withColumn("rank", rank().over(Window.orderBy(col("totalMeasures").desc()))).show();
    }

    /**
     * Rank stations by rainfall
     *
     * @param weatherStationsData
     */
    public void rankStationsByRainfall(final Dataset<Row> weatherStationsData) {
        Dataset<Row> df2 = weatherStationsData.groupBy("station").agg(avg("rain").alias("avgRain")).select("*");
        df2.withColumn("rank", rank().over(Window.orderBy(col("avgRain").desc()))).show();
    }

    /**
     * Rank stations by sunshine
     *
     * @param weatherStationsData
     */
    public void rankStationsBySunshine(final Dataset<Row> weatherStationsData) {
        Dataset<Row> avgSunshineDataSet = weatherStationsData.groupBy("station").agg(avg("sunshine").alias("avgSunshine")).select("*");
        avgSunshineDataSet.withColumn("rank", rank().over(Window.orderBy(col("avgSun").desc()))).show();
    }

    /**
     * Display worst rain fall for each station
     *
     * @param weatherStationsData
     */
    public void displayWorstRainfall(final Dataset<Row> weatherStationsData) {
        Dataset<Row> worstRainfallDataset = weatherStationsData.groupBy(col("station")).agg(min(col("rain")).alias("worstRainfall")).orderBy(min(col("rain")));
        worstRainfallDataset.filter(worstRainfallDataset.col("worstRainfall").gt(0.0)).show();
    }

    /**
     * Display best sunshine of each station
     *
     * @param weatherStationsData
     */
    public void displayBestSunshine(final Dataset<Row> weatherStationsData) {
        weatherStationsData.groupBy("station").agg(max("sunShine").alias("bestSunshine")).orderBy(max(col("sunShine")).desc()).show();
    }

    /**
     * Display averages for May across all stations
     *
     * @param weatherStationsData
     */
    public void displayAllStationsAvgAcrossMay(final Dataset<Row> weatherStationsData) {
        weatherStationsData.groupBy("month").agg(avg("rain"), avg("sunshine"), avg("af")).where(col("month").equalTo(Integer.parseInt("5"))).show();
    }
}

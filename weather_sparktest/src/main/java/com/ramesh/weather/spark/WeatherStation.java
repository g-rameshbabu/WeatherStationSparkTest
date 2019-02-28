package com.ramesh.weather.spark;


import org.apache.spark.api.java.JavaSparkContext;


public interface WeatherStation {

    void parseWeatherStationData();

    void cleanExistingData();

    void loadWeatherStationData();

    void convertToWeatherStationData(JavaSparkContext sparkContext);

}

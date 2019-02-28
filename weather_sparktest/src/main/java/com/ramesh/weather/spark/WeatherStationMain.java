package com.ramesh.weather.spark;

import com.ramesh.weather.spark.impl.WeatherStationImpl;
import com.ramesh.weather.spark.utils.SparkContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;

import javax.inject.Inject;

@Slf4j
public class WeatherStationMain {
    private static final String APPLICATION_NAME = "SparkWeatherStationApp";

    public static void main(String[] args) {

        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName(APPLICATION_NAME)
                .setMaster("local");
        WeatherStation weatherStation = new WeatherStationImpl();
        weatherStation.cleanExistingData();
        weatherStation.loadWeatherStationData();
        weatherStation.parseWeatherStationData();
        weatherStation.convertToWeatherStationData(SparkContext.javaSparkContext(sparkConf));
        SparkContext.javaSparkContext(sparkConf).stop();
    }


}

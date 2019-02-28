package com.ramesh.weather.spark.impl;

import com.ramesh.weather.spark.WeatherStation;
import com.ramesh.weather.spark.beans.WeatherStationData;
import com.ramesh.weather.spark.helper.WeatherStationServiceHelper;
import com.ramesh.weather.spark.utils.SparkContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class WeatherStationImpl implements WeatherStation {

    public static final String URL_WEATHER_STATIONDATA = "https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/";
    public static final String WEATHER_STATION_DATA = "weatherStationData";
    private transient SQLContext sqlContext;
    //Few stations commented as these are having some special data,which needs be handled.
    private static final String[] stationsArray = {"aberporth", "armagh", "ballypatrick", "bradford", "braemar", "camborne", "cambridge", "cardiff",
            "chivenor", "cwmystwyth", "dunstaffnage", "durham", "eastbourne", "eskdalemuir", "heathrow",
            "hurn", "lerwick", "leuchars"/*, "lowestoft"*/, "manston", "nairn", "newtonrigg",
            "oxford", "paisley", "ringway", "rossonwye", "shawbury", "sheffield", "southampton",
            "stornoway", "suttonbonington", "tiree", "valley", "waddington"/*, "whitby", "wickairport"*/, "yeovilton"};


    @Override
    public void parseWeatherStationData() {
        final List<String> stations = Arrays.asList(stationsArray);
        stations.stream().forEach(station -> {
            try {
                File sourceFile = new File("src/main/resources/source/" + station + "data.txt");
                File outputFile = new File("src/main/resources/output/" + station + "data.txt");
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile));
                //At the moment skipping to 8 ,this logic can be changed to find changing header length and skip accordingly
                try (Stream<String> linesStream = Files.lines(sourceFile.toPath()).skip(8)) {
                    linesStream.filter(line -> !(line.contains("Site Closed") || line.contains("Site closed"))).forEach(lineData -> {
                        try {
                            //if (!(lineData.contains("Site Closed") || lineData.contains("Site closed"))) {
                            bufferedWriter.write(station);
                            bufferedWriter.write(",");
                            String replacedString = lineData.replaceAll("\\s+", ",").replaceFirst(",", "").replaceAll("\\*", "").replaceAll("\\#", "").replaceAll("Provisional", "");
                            bufferedWriter.write(replacedString);
                            bufferedWriter.newLine();
                        } catch (IOException io) {
                            log.error("Exception occurred while writing into BufferedWriter {}", io);
                        } catch (Exception e) {
                            log.error("Exception occurred during formatting the data {}", e);
                        }

                    });
                } finally {
                    try {
                        bufferedWriter.close();
                    } catch (IOException ex) {
                        log.error("Exception occurred while closing buffered writer {}", ex);
                    }
                }
            } catch (IOException io) {
                log.error("Exception occurred while formatting weather station data {}", io);
            } catch (Exception ex) {
                log.error("Exception occurred while formatting weather station data {}", ex);
            }

        });
    }

    @Override
    public void cleanExistingData() {
        try {
            log.info("Cleaning the directory for the latest data");
            FileUtils.cleanDirectory(new File("src/main/resources/source/"));
            FileUtils.cleanDirectory(new File("src/main/resources/output/"));
            log.info("Cleaning successfully completed");
        } catch (IOException e) {
            log.error("Exception occurred while cleaning the directory {}", e);
        }
    }

    @Override
    public void loadWeatherStationData() {

        final List<String> stations = Arrays.asList(stationsArray);
        stations.stream().forEach(station -> {
            String url = URL_WEATHER_STATIONDATA + station + "data.txt";
            String fileName = "src/main/resources/source/" + station + "data.txt";
            try {
                FileUtils.copyURLToFile(
                        new URL(url),
                        new File(fileName),
                        5000,
                        6000);
            } catch (IOException e) {
                log.error("There is an error while reading the file", e);
            }
        });
    }

    @Override
    public void convertToWeatherStationData(final JavaSparkContext javaSparkContext) {
        String formattedWeatherStationData = "./src/main/resources/output/*";

        try {
            // Load a text file and convert each line to a JavaBean.
            JavaRDD<WeatherStationData> weatherStationsData = javaSparkContext.textFile(formattedWeatherStationData).map(
                    (String line) -> {
                        String[] columnData = line.split(",");
                        WeatherStationData weatherStationData = new WeatherStationData();
                        weatherStationData.setStation(columnData[0]);
                        weatherStationData.setYear(Integer.parseInt(columnData[1]));
                        weatherStationData.setMonth(Integer.parseInt(columnData[2]));
                        weatherStationData.setTMax(columnData[3].equals("---") ? null : Double.parseDouble(columnData[3]));
                        weatherStationData.setTMin(columnData[4].equals("---") ? null : Double.parseDouble(columnData[4]));
                        weatherStationData.setAf(columnData[5].equals("---") ? null : Integer.parseInt(columnData[5]));
                        weatherStationData.setRain(columnData[6].equals("---") ? null : Double.parseDouble(columnData[6]));
                        weatherStationData.setSunShine(columnData[7].equals("---") ? null : Double.parseDouble(columnData[7]));

                        return weatherStationData;

                    }
            );
            sqlContext = SparkContext.sqlContext(javaSparkContext);
            // Apply a schema to an RDD of JavaBeans and register it as a table.
            Dataset<Row> weatherStationDataFrame = sqlContext.createDataFrame(weatherStationsData, WeatherStationData.class);
            weatherStationDataFrame.registerTempTable(WEATHER_STATION_DATA);
        } catch (Exception ex) {
            log.error("Exception occurred while converting to WeatherStationData");
        }
        sqlContext.cacheTable(WEATHER_STATION_DATA);
        Dataset<Row> result = sqlContext.sql("SELECT * FROM weatherStationData");
        try {
            WeatherStationServiceHelper.rankStationsByOnline(result);
            WeatherStationServiceHelper.rankStationsByRainfall(result);
            WeatherStationServiceHelper.rankStationsBySunshine(result);
            WeatherStationServiceHelper.displayWorstRainfall(result);
            WeatherStationServiceHelper.displayBestSunshine(result);
            WeatherStationServiceHelper.displayAllStationsAvgAcrossMay(result);
        } catch (Exception ex) {
            log.error("Exception occurred while processing the dataFrame");
        }
        sqlContext.uncacheTable(WEATHER_STATION_DATA);

    }


}

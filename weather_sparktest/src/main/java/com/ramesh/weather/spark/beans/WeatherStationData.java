package com.ramesh.weather.spark.beans;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@Data
public class WeatherStationData implements Serializable {
    private static final long serialVersionUID = 1L;
    private String station;
    private Integer year;
    private Integer month;
    private Double tMax;
    private Double tMin;
    private Integer af;
    private Double rain;
    private Double sunShine;
}

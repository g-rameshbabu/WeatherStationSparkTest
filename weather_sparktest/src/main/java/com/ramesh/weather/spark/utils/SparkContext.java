package com.ramesh.weather.spark.utils;

import lombok.experimental.UtilityClass;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;  
import org.apache.spark.sql.SparkSession;  

@UtilityClass
public class SparkContext<T> {  
    private transient static JavaSparkContext javaSparkContext;
    private transient static SparkSession sparkSession;
    private transient static SQLContext sqlContext;
      
  
    public JavaSparkContext javaSparkContext(final SparkConf sparkConf) {
        if (javaSparkContext == null)  
            javaSparkContext = new JavaSparkContext(sparkConf);
        return javaSparkContext;  
    }

    public SparkSession sparkSession() {
        if (sparkSession == null)
            sparkSession = SparkSession.builder().getOrCreate();
        return sparkSession;
    }

    public SparkSession sparkSession(final String appName) {
        if (sparkSession == null)  
            sparkSession = SparkSession.builder().appName(appName).getOrCreate();
        return sparkSession;  
    }  
  
    public SQLContext sqlContext() {
        if (sqlContext == null)  
            sqlContext = sparkSession().sqlContext();
        return sqlContext;  
    }

    public SQLContext sqlContext(JavaSparkContext javaSparkContext) {
        if (sqlContext == null)
            sqlContext = new SQLContext(javaSparkContext);
        return sqlContext;
    }

}  
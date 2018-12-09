package edu.ucr.cs.cs226.groupF;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.*;

public class CrimeAnalyze {

    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.ERROR);
        String file_path = "file:///home/jonathan/";

        long startTime = System.currentTimeMillis();
        long endTime, totalTime;
        //SparkSession session = SparkSession.builder().appName("CrimeSolution").getOrCreate();
        SparkSession session = SparkSession.builder().appName("CrimeSolution").master("local[*]").getOrCreate();

        //String file_path = "file:///opt/hadoop/home/hadoop7/";

        //SparkSession session = SparkSession.builder().appName("CrimeSolution").getOrCreate();

        //when reading the csv file to the dataset as rows, we set the read in delimeter as tab since we use tab to
        //seperate each column and set the let the first row of the csv file as the schema.
        Dataset<Row> CombineData = session.read().option("delimiter","\t").option("header", "true").csv(file_path+"Chicago_Crimes.csv");


        //CombineData.printSchema();
/*
        CombineData.createOrReplaceTempView("CombineData");
        Dataset<Row> fa = session.sqlContext().sql("Select `Primary Type` From CombineData ");
        fa.write().format("com.databricks.spark.csv").option("header", "true").save("/home/jonathan/csv");
        //fa.groupBy("Primary Type").count().show();
*/
        //System.out.println("The Type of crime for the total year");



        CombineData.groupBy("Primary Type").count().orderBy(desc("count")).show(40, false);
        //Dataset<Row> ab  = CombineData.groupBy("Primary Type").count().orderBy("count");
            //ab.write().format("com.databricks.spark.csv").option("header", "true").save("/home/jonathan/csv");
        //ab.coalesce(2).write().option("header", "true").csv("/home/jonathan/csv1");



        System.out.println("Total Crime each year");
        CombineData.groupBy("Year").count().orderBy("Year").show(20,false);



        Dataset<Row> filterData = CombineData.withColumn("Year" ,col("Year").cast("integer"));
        //System.out.println("Print the filter schema");
            //filterData.printSchema();
            //System.out.println("Crime each year without null");
            //Dataset<Row> filterData = CombineData.select("Year").filter("Year != 'True' and Year != 'False'");


        
        System.out.println("Print out number of arrest for each year");
        filterData.where("Arrest == 'True'").groupBy("Year").count().orderBy("Year").show(false);

        System.out.println("Print out the number of crimes type for each year");
        Dataset<Row> CrimePerYear = filterData.groupBy("Primary Type", "Year").count().orderBy("Year", "Primary Type");
        CrimePerYear.coalesce(1).write().option("header","true").csv(file_path + "groupF/CrimePerYear");
        filterData.groupBy("Primary Type", "Year").count().orderBy("Year", "Primary Type").show(false);

        System.out.println("Print out the frequent location that crime occur");
        filterData.groupBy("Location Description").count().orderBy(desc("count")).show(200,false);
        Dataset<Row> freqLoc = filterData.groupBy("Location Description").count().orderBy(desc("count"));
        freqLoc.coalesce(1).write().option("header","true").csv(file_path + "groupF/FreqLoc");


        System.out.println("Calculate the number of crime x base on location ");
        Dataset<Row> CrimePerLoc = filterData.groupBy("Primary Type", "Location Description").count().sort("Primary Type", "Location Description");
        CrimePerLoc.coalesce(1).write().option("header","true").csv(file_path + "groupF/CrimePerLoc");
        filterData.groupBy("Primary Type", "Location Description").count().sort("Primary Type", "Location Description").show(false);


        System.out.println("Amount of Theft each year");
        filterData.filter(CombineData.col("Primary Type").equalTo("THEFT")).groupBy("Year").count().orderBy("Year").show();


        System.out.println("Amount of Homicide each year");
        filterData.filter(filterData.col("Primary Type").equalTo("HOMICIDE")).groupBy("Year").count().orderBy("Year").show();

        System.out.println("Amount of Criminal Sexual Assault + Sex Offense each year");
        Dataset<Row> offense = filterData.filter(filterData.col("Primary Type").equalTo("CRIM SEXUAL ASSAULT")).union(filterData.filter(filterData.col("Primary Type").equalTo("SEX OFFENSE")))
                .groupBy("Primary Type","Year").count().orderBy("Primary Type", "Year");
        offense.coalesce(1).write().option("header","true").csv(file_path + "groupF/offense");
        offense.show(false);

        endTime = System.currentTimeMillis();

        totalTime = (endTime - startTime)/1000;
        double sec = totalTime%60;
        long min = totalTime / 60;
        System.out.println("Total execution time : " + min + " minutes and " + sec +" seconds");





    }
}

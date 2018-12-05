package edu.ucr.cs.cs226.groupF;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class CrimeAnalyze {

    public static void main(String[] args){

        List<String> rows = new ArrayList<>();
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("CrimeSolution").master("local[*]").getOrCreate();

        //when reading the csv file to the dataset as rows, we set the read in delimeter as tab since we use tab to
        //seperate each column and set the let the first row of the csv file as the schema.
        Dataset<Row> CombineData = session.read().option("delimiter","\t").option("header", "true").csv("/home/jonathan/Chicago_Crimes.csv");


        //CombineData.printSchema();

        System.out.println("The Type of crime for the total year");
        CombineData.groupBy("Primary Type").count().orderBy(desc("count")).show(40, false);



        System.out.println("Total Crime each year");
        CombineData.groupBy("Year").count().orderBy("Year").show(20,false);



        Dataset<Row> filterData = CombineData.withColumn("Year" ,col("Year").cast("integer"));
        //System.out.println("Print the filter schema");
        //filterData.printSchema();
        //System.out.println("Crime each year without null");
        //Dataset<Row> filterData = CombineData.select("Year").filter("Year != 'True' and Year != 'False'");


        
        System.out.println("Print out number of arrest for each year");
        //filterData.groupBy("Arrest","Year").count().show();
        filterData.where("Arrest == 'True'").groupBy("Year").count().orderBy("Year").show();

        System.out.println("Print out the number of crimes type for each year");
        filterData.groupBy("Primary Type", "Year").count().orderBy("Year", "Primary Type").show(600,false);


        System.out.println("Print out the frequent location that crime occur");
        filterData.groupBy("Location Description").count().orderBy("count").show(500,false);



        System.out.println("Calculate the number of crime x base on location ");
        filterData.groupBy("Primary Type", "Location Description").count().sort("Primary Type", "Location Description").show
                (2500,false);



        System.out.println("Amount of Theft each year");
        filterData.filter(CombineData.col("Primary Type").equalTo("THEFT")).groupBy("Year").count().orderBy("Year").show();


        System.out.println("Amount of Homicide each year");
        filterData.filter(filterData.col("Primary Type").equalTo("HOMICIDE")).groupBy("Year").count().orderBy("Year").show();

        System.out.println("Amount of Crim sexual assualt + Sex Offense each year");
        filterData.filter(filterData.col("Primary Type").equalTo("CRIM SEXUAL ASSAULT")).union(filterData.filter(filterData.col("Primary Type").equalTo("SEX OFFENSE")))
                .groupBy("Primary Type","Year").count().orderBy("Primary Type", "Year").show(40,false);




        //===== old data=====//

//        long total_crimes  = CombineData.select("*").count();
//        System.out.println(total_crimes);

/*
        //#2
        //System.out.println("Number of Crime Type in 2012-2017");
        Dataset<Row> response12_17 = session.read().option("header", "true").csv("/home/jonathan/in/Chicago_Crimes_2012_" +
                "to_2017.csv");
        //RelationalGroupedDataset groupCrimeType12_17 = response12_17.groupBy(col("Primary Type"));
        //groupCrimeType12_17.count().show(40,false);
        /print the header
        //response12_17.printSchema();
        //#2
        //System.out.println("Number of Crime Type in 2008-2011");
        Dataset<Row> response08_11 = session.read().option("header", "true").csv("in/Chicago_Crimes_2008_" +
                "to_2011.csv");
        //RelationalGroupedDataset groupCrimeType08_11 = response08_11.groupBy(col("Primary Type"));
        //groupCrimeType08_11.count().show(40,false);
        //#2
        //System.out.println("Number of Crime Type in 2005-2007");
        Dataset<Row> response05_07 = session.read().option("header", "true").csv("in/Chicago_Crimes_2005_" +
                "to_2007.csv");
        //RelationalGroupedDataset groupCrimeType05_07 = response05_07.groupBy(col("Primary Type"));
        //The truncate:false will not make the printed table all shorten
        //need to set > 20 since function will only print 20 rows
        //groupCrimeType05_07.count().show(40,false);
        //#4
        //The combine dataset
        //System.out.println("Number of Each Crime Type in 2008-2017");
        Dataset<Row> CombineSet = response12_17.union(response08_11);
        //RelationalGroupedDataset groupCombineCrime = CombineSet.groupBy(col("Primary Type"));
        //groupCombineCrime.count().show(40,false);
        //System.out.println("The combine dataset to count how many crime type for the whole year #4==========================");
        //CombineSet.groupBy("Primary Type").count().show(40, false);
        //System.out.println("The combine dataset to count how many crime each year==========================");
        //CombineSet.groupBy("Year").count().show(40, false);
        //System.out.println("The combine dataset to count how many crime type each year #5==========================");
        //CombineSet.groupBy("Year","Primary Type").count().sort("Year", "Primary Type").show(600, false);
        System.out.println("Calculate how many crime base on location ");
        CombineSet.groupBy("Location Description").count().show(40, false);
        System.out.println("Calculate the number of crime x base on location ");
        CombineSet.groupBy("Primary Type", "Location Description").count().sort("Primary Type").show
                (40,false);
        //Count the total crime in each year
        long total_crime12_17  = response12_17.select("*").count();
        long total_crime08_11  = response08_11.select("*").count();
        System.out.println("The Total crime from 2012 to 2017 is : " + total_crime12_17);
        System.out.println("The Total crime from 2008 to 2011 is : " + total_crime08_11);
           */





    }
}

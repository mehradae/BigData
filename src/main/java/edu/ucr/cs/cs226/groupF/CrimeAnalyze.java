package edu.ucr.cs.cs226.groupF;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.*;

public class CrimeAnalyze {

    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.ERROR);
        String file_path = args[0];

        //start the timer for execution time
        long startTime = System.currentTimeMillis();
        long endTime, totalTime;
        SparkSession session = SparkSession.builder().appName("CrimeSolution").getOrCreate();
        //SparkSession session = SparkSession.builder().appName("CrimeSolution").master("local[*]").getOrCreate();


        //when reading the csv file to the dataset as rows, we set the read in delimeter as tab since we use tab to
        //separate each column and set the let the first row of the csv file as the schema.
        Dataset<Row> CombineData = session.read().option("delimiter","\t").option("header", "true").csv(file_path+"Chicago_Crimes.csv");


        //this would print out the schema of the dataset
        //CombineData.printSchema();

        //group the dataset base on primary type and get the amount for each type
        //Then it will sort by count in descending order and show at most 40 rows and disable the truncate when printing the row
        System.out.println("The Type of crime for the total year");
        CombineData.groupBy("Primary Type").count().orderBy(desc("count")).show(40, false);


        //Group the dataset base on the year of the crime and get the amount of each crime each year
        System.out.println("Total Crime each year");
        CombineData.groupBy("Year").count().orderBy("Year").show(20,false);


        //Create a new dataset that would change the Year column type into integer
        Dataset<Row> filterData = CombineData.withColumn("Year" ,col("Year").cast("integer"));

        //This would filter the dataset where the Arrest column field is true and group them by the year
        //and it will display the amount arrest there is over the years
        System.out.println("Print out number of arrest for each year");
        filterData.where("Arrest == 'True'").groupBy("Year").count().orderBy("Year").show(false);

        //Group the dataset by their Primary Type and Year and the count for each primary type in each year.
        //Then print them out by the order of year then primary type, and write them into a csv file with 1 partition.
        System.out.println("Print out the number of crimes type for each year and write into CrimePerYear folder as  csv file");
        Dataset<Row> CrimePerYear = filterData.groupBy("Primary Type", "Year").count().orderBy("Year", "Primary Type");
        CrimePerYear.coalesce(1).write().option("header","true").csv(file_path + "groupF/CrimePerYear");
        filterData.groupBy("Primary Type", "Year").count().orderBy("Year", "Primary Type").show(false);


        //Group the dataset by their location description column and the count for each location description.
        //Then print them out by the order of the count in descending order, and write them into csv file with 1 partition.
        System.out.println("Print out the frequent location that crime occur and write into FrequentLoc folder as csv file");
        filterData.groupBy("Location Description").count().orderBy(desc("count")).show(200,false);
        Dataset<Row> freqLoc = filterData.groupBy("Location Description").count().orderBy(desc("count"));
        freqLoc.coalesce(1).write().option("header","true").csv(file_path + "groupF/FrequentLoc");


        //Group the dataset by their Primary Type and Location Description and the count for each primary type in each location description.
        //Then print them out by the order of primary type then location description, and write them into csv file with 1 partition.
        System.out.println("Calculate the number of crime x base on location and write into CrimePerLoc folder as csv file");
        Dataset<Row> CrimePerLoc = filterData.groupBy("Primary Type", "Location Description").count().sort("Primary Type", "Location Description");
        CrimePerLoc.coalesce(1).write().option("header","true").csv(file_path + "groupF/CrimePerLoc");
        filterData.groupBy("Primary Type", "Location Description").count().sort("Primary Type", "Location Description").show(false);


        //Filter the dataset with Primary Type column that have Theft and group them by years and get the count of
        //theft for each year, and show the year with amount of theft each year in order by the year.
        System.out.println("Amount of Theft each year");
        filterData.filter(CombineData.col("Primary Type").equalTo("THEFT")).groupBy("Year").count().orderBy("Year").show();


        //Filter the dataset with Primary Type column that have Homicide and group them by years and get the count of
        //theft for each year, and show the year with amount of homicide each year in order by the year.
        System.out.println("Amount of Homicide each year");
        filterData.filter(filterData.col("Primary Type").equalTo("HOMICIDE")).groupBy("Year").count().orderBy("Year").show();


        //Get two dataset that one is filter with Primary Type column that have Crim sexual assault and another one with sex offense
        //Then get the union dataset of those two and group them by Primary Type and Year.
        //It will be written into a csv file and show it
        System.out.println("Amount of Criminal Sexual Assault + Sex Offense each year and write into SexualOffense folder as csv file");
        Dataset<Row> offense = filterData.filter(filterData.col("Primary Type").equalTo("CRIM SEXUAL ASSAULT")).union(filterData.filter(filterData.col("Primary Type").equalTo("SEX OFFENSE")))
                .groupBy("Primary Type","Year").count().orderBy("Primary Type", "Year");
        offense.coalesce(1).write().option("header","true").csv(file_path + "groupF/SexualOffense");
        offense.show(false);

        //get the time when spark execution end
        endTime = System.currentTimeMillis();

        //Get the total execution time to change from ms to second
        totalTime = (endTime - startTime)/1000;
        //calculate the second and minute from the totalTime
        double sec = totalTime%60;
        long min = totalTime / 60;
        System.out.println("Total execution time : " + min + " minutes and " + sec +" seconds");




    }
}

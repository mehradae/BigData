Run Spark 

User need to input one parameter for running the project. The user need to provide the file_path of Chicago_Crimes.csv file

//For local
./bin/spark-submit --master local[*] --executor-memory 7g --driver-memory 7g --class edu.ucr.cs.cs226.groupF.CrimeAnalyze /home/jonathan/workspace/CrimeAnalyze/target/CrimeAnalyze-1.0-SNAPSHOT.jar file:///home/jonathan/


//For cluster
start the master for spark ui

./sbin/start-master.sh

./bin/spark-submit --master spark://jonathan-VirtualBox:7077 --executor-memory 7g --driver-memory 7g --class edu.ucr.cs.cs226.groupF.CrimeAnalyze /home/jonathan/workspace/CrimeAnalyze/target/CrimeAnalyze-1.0-SNAPSHOT.jar



The output csv locations will be at the provided file path of Chicago_Crimes.csv and into groupF folder

For example:
The Chicago_Crimes.csv file path is "file:///home/jonathan/", so the output path of the csv file will be at the  "file:///home/jonathan/groupF/..."

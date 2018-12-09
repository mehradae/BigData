Run Spark 

//For local
./bin/spark-submit --master local[*] --executor-memory 7g --driver-memory 7g --class edu.ucr.cs.cs226.groupF.CrimeAnalyze /home/jonathan/workspace/CrimeAnalyze/target/CrimeAnalyze-1.0-SNAPSHOT.jar


//For cluster
start the master for spark ui

./sbin/start-master.sh

./bin/spark-submit --master spark://jonathan-VirtualBox:7077 --executor-memory 7g --driver-memory 7g --class edu.ucr.cs.cs226.groupF.CrimeAnalyze /home/jonathan/workspace/CrimeAnalyze/target/CrimeAnalyze-1.0-SNAPSHOT.jar




#just for testing purposes, copies a file into a hadoop instance on this machine.

mvn -DskipTests clean package

hadoop fs -rm -r hdfs://localhost:8020/d*
hadoop fs -put target/di*shaded.jar hdfs://localhost:8020/dp.jar


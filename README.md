distributed_parsing
===================

Toying with parsing files within Yarn.

Doesnt do much of anything right now. Its a real big mess.

I'm using Kafka 0.8 atm, which can be gotten via a dl at:
http://kafka.apache.org/downloads.html

To stick it into your local maven repo:
mvn install:install-file -Dfile=kafka_2.8.0-0.8.0-beta1.jar -DgroupId=org.apache.kafka -DartifactId=kafka -Dversion=0.8 -Dpackaging=jar

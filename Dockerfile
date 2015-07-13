FROM java:8 

# Install maven
RUN apt-get update
RUN apt-get install -y maven

WORKDIR /code

# Prepare by downloading dependencies
ADD pom.xml /code/pom.xml
RUN ["mvn", "dependency:resolve"]
RUN ["mvn", "verify"]

# Adding source, compile and package into a fat jar
ADD src /code/src
RUN ["mvn", "package"]

CMD /usr/lib/jvm/java-8-openjdk-amd64/bin/java -cp target/dataunity-filemetadata-0.0.1-SNAPSHOT.jar dataunity.filemetadata.worker.KafkaConsumer

# OLD VERSION, slow to build
# FROM maven:3.2-jdk-7-onbuild
# EXPOSE 5004
# CMD java -cp target/dataunity-filemetadata-0.0.1-SNAPSHOT.jar dataunity.filemetadata.worker.KafkaConsumer
## CMD java -cp target/dataunity-filemetadata-0.0.1-SNAPSHOT.jar dataunity.filemetadata.worker.Main
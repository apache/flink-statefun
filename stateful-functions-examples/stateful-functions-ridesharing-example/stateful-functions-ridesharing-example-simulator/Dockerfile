# Start with a base image containing Java runtime
FROM openjdk:8-jdk-alpine

# Add a volume pointing to /tmp
VOLUME /tmp

# Make port 5656 available to the world outside this container
EXPOSE 5656

ADD target/*-simulator-*.jar simulator.jar
ADD application.yaml application.yaml 

# Run the jar file
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-Dspring.profiles.active=dev","-Dspring.config.location=file:/application.yaml","-jar","/simulator.jar"]

FROM maven:3-jdk-8 as builder
WORKDIR /maven-app
COPY . .
RUN mvn install

# ENV
FROM openjdk:8-jre-alpine
EXPOSE 3000
COPY --from=builder /maven-app/target/*-jar-with-dependencies.jar /app/mdm-importer.jar
COPY ./mdm-test.hahnpro.com.p12 /app
WORKDIR /app
CMD echo "Starting Redirect Service" && \
    java  -jar /app/mdm-importer.jar -Djava.security.egd=file:/dev/./urandom

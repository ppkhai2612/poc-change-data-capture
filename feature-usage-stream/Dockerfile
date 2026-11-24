# Multi-stage build for Java application

# Stage 1: Build the application with Maven
FROM maven:3.9-amazoncorretto-21 AS build
WORKDIR /app
# Copy pom.xml first for better layer caching
COPY pom.xml .
# Copy source code
COPY src ./src
# Package the application
RUN mvn clean package

# Stage 2: Create a minimal runtime image
FROM amazoncorretto:21-alpine
WORKDIR /app
# Copy the JAR file from the build stage
COPY --from=build /app/target/feature-usage-stream-1.0-SNAPSHOT.jar ./app.jar
# Set the entrypoint to run the application
ENTRYPOINT ["java", "-jar", "app.jar"]
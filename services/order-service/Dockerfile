# Build stage: compiles the order-service module against the parent POM
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY services/order-service/pom.xml services/order-service/pom.xml
COPY services/order-service/src services/order-service/src
RUN mvn -q -pl services/order-service -am -DskipTests package

# Runtime stage
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/services/order-service/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]

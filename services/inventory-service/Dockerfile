# Build stage: compiles the inventory-service module against the parent POM.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY services/order-service/pom.xml services/order-service/pom.xml
COPY services/payment-service/pom.xml services/payment-service/pom.xml
COPY services/inventory-service/pom.xml services/inventory-service/pom.xml
COPY services/notification-service/pom.xml services/notification-service/pom.xml
COPY services/inventory-service/src services/inventory-service/src
COPY contracts contracts
RUN mvn -q -pl services/inventory-service -am -DskipTests package

# Runtime stage
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/services/inventory-service/target/*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]

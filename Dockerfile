FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
COPY --from=build /workspace/target/*.jar /app.jar

ENTRYPOINT ["java", "-jar", "/app.jar"]

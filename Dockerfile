FROM eclipse-temurin:21
LABEL authors="anassgassi"
WORKDIR /app
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java","-XX:MaxRAMPercentage=75","-jar","app.jar"]
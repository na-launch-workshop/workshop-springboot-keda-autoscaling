# Spring Boot

This project uses Spring Boot with Spring Kafka.

If you want to learn more about Spring Boot, please visit its website: <https://spring.io/projects/spring-boot>.

## Running the application in dev mode

You can run your application in dev mode using:

```shell script
./mvnw spring-boot:run
```

## Packaging and running the application

The application can be packaged using:

```shell script
./mvnw package
```

It produces the `workshop-springboot-keda-autoscaling-1.0.0-SNAPSHOT.jar` file in the `target/` directory.

The application is now runnable using:

```shell script
java -jar target/workshop-springboot-keda-autoscaling-1.0.0-SNAPSHOT.jar
```

## Related Guides

- Spring Boot ([guide](https://spring.io/projects/spring-boot)): Build the application
- Spring Kafka ([guide](https://spring.io/projects/spring-kafka)): Publish and consume Kafka records
- KEDA Kafka scaler ([guide](https://keda.sh/docs/latest/scalers/apache-kafka/)): Scale deployments from Kafka lag

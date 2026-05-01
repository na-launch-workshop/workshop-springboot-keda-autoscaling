package com.redhat;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KedaApplication {

    public static void main(String[] args) {
        SpringApplication.run(KedaApplication.class, args);
    }
}

package com.anilyanai.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(
        scanBasePackages = "com.anilyanai.application"
)
public class CollabServerMain {
    public static void main(String[] args) {
        SpringApplication.run(CollabServerMain.class, args);
    }
}

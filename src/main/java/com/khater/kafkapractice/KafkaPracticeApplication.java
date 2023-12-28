package com.khater.kafkapractice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.khater.kafkapractice.config.KafkaConfigProps;
import com.khater.kafkapractice.domain.CustomerVisitEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.UUID;

@SpringBootApplication
public class KafkaPracticeApplication {

	@Autowired
	private ObjectMapper objectMapper;

	public static void main(String[] args) {
		SpringApplication.run(KafkaPracticeApplication.class, args);
	}

	@Bean
	public ApplicationRunner runner(final KafkaTemplate<String , String> kafkaTemplate, final KafkaConfigProps kafkaConfigProps){
		final CustomerVisitEvent event = CustomerVisitEvent.builder()
				.customerId(UUID.randomUUID().toString())
				.dateTime(LocalDateTime.now())
				.build();

		return args -> kafkaTemplate.send(kafkaConfigProps.getTopic() , objectMapper.writeValueAsString(event));
	}


	@KafkaListener(topics = "customer.visit")
	public String listen(final String in){
		System.out.println(in);
		return in;
	}
}

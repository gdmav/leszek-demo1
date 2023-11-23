package com.example.demo1

import com.github.javafaker.Faker
import com.luxoft.lmd.User
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import jakarta.annotation.PostConstruct
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Duration

@SpringBootApplication
@EnableScheduling
class Demo1Application

fun main(args: Array<String>) {
	runApplication<Demo1Application>(*args)
}


@Configuration
class AppConfig {
	@Bean fun kafkaProducer(): KafkaProducer<String, Any> {
		val config = mapOf<String, String>(
			ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
			ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
			ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java.name,
			KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to "http://localhost:8085"
		)

		return KafkaProducer(config)
	}

	@Bean fun kafkaConsumer(): KafkaConsumer<String, GenericRecord> {
		val config = mapOf<String, String>(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java.name,
			KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to "http://localhost:8085",
			ConsumerConfig.GROUP_ID_CONFIG to "consumer-group-01",
			KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to "false",
			ConsumerConfig.FETCH_MIN_BYTES_CONFIG to "200000",
			ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG to "5000"
		)

		return KafkaConsumer(config)
	}
}

@Component
class DataProducer(
	val kafkaProducer: KafkaProducer<String, Any>
) {
	val logger = KotlinLogging.logger { }

	@Scheduled(fixedDelay = 1000)
	fun run() {
		val payload =
			User.newBuilder()
				.setAge(10)
				.setUsername(Faker.instance().idNumber().ssnValid())
				.setFirstName(Faker.instance().name().firstName())
				.setLastName(Faker.instance().name().lastName())
				.setTeam("LMD")
				.build()

		logger.warn { "-> $payload" }
		val record = ProducerRecord<String, Any>("users1", payload)
		val result = kafkaProducer.send(record).get()
	}
}

@Component
class DataConsumer(
	val kafkaConsumer: KafkaConsumer<String, GenericRecord>
) : Runnable {
	val logger = KotlinLogging.logger { }

	@PostConstruct
	fun init() {
		Thread(this).start()
	}

	override fun run() {
		kafkaConsumer.subscribe(listOf("users1"))

		logger.error { "started listening for messages" }
		while (true) {
			val records = kafkaConsumer.poll(Duration.ofSeconds(10))
			logger.info { "got ${records.count()} records" }

			records.forEach { record ->
				logger.error { "got result: ${record.value()::class.java}" }
				logger.error { "got result: ${record.value()}" }
			}
		}
	}
}
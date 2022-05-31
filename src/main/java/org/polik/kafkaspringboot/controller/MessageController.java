package org.polik.kafkaspringboot.controller;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.polik.kafkaspringboot.domain.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

/**
 * Created by Polik on 5/30/2022
 */
@RestController
@RequestMapping("/api/messages")
public class MessageController {
    @Value("${kafka.topic.request}")
    private String requestTopic;

    private final ReplyingKafkaTemplate<String, String, String> kafkaTemplate;

    public MessageController(ReplyingKafkaTemplate<String, String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    @SneakyThrows
    public String publish(@RequestBody Message message) {

        ProducerRecord<String, String> record = new ProducerRecord<>(requestTopic, message.message());
        RequestReplyFuture<String, String, String> replyFuture = kafkaTemplate.sendAndReceive(record);
        ConsumerRecord<String, String> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);

        return consumerRecord.value();
    }
}

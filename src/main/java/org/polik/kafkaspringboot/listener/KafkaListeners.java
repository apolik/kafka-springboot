package org.polik.kafkaspringboot.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

/**
 * Created by Polik on 5/30/2022
 */
@Component
@Slf4j
public class KafkaListeners {
    @KafkaListener(topics = "${kafka.topic.request}", containerFactory = "kafkaListenerContainerFactory")
    @SendTo("${kafka.topic.reply}")
    public String listen(String data) {
        log.info("Received: {}", data);
        return String.format("Sent and received: %s", data);
    }
}

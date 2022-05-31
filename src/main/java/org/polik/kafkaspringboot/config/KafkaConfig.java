package org.polik.kafkaspringboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Polik on 5/30/2022
 */
@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.reply}")
    private String replyTopic;

    @Value("${kafka.topic.request}")
    private String requestTopic;
    public final static String CONSUMER_GROUPS = "listener#1";


    public Map<String, Object> consumerConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUPS);

        return map;
    }

    public Map<String, Object> producerConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return map;
    }

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyKafkaTemplate() {
        ReplyingKafkaTemplate<String, String, String> requestReplyKafkaTemplate =
                new ReplyingKafkaTemplate<>(requestProducerFactory(),
                        replyListenerContainer());
        requestReplyKafkaTemplate.setDefaultTopic(replyTopic);
        requestReplyKafkaTemplate.setDefaultReplyTimeout(Duration.ofSeconds(10));
        return requestReplyKafkaTemplate;
    }

    @Bean
    public ProducerFactory<String, String> requestProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public ConsumerFactory<String, String> replyConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> replyListenerContainer() {
        ContainerProperties containerProperties = new ContainerProperties(replyTopic);
        return new KafkaMessageListenerContainer<>(replyConsumerFactory(), containerProperties);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(replyConsumerFactory());
        factory.setReplyTemplate(replyKafkaTemplate());

        return factory;
    }

    @Bean
    public NewTopic replyTopic() {
        return TopicBuilder
                .name(replyTopic)
                .build();
    }

    @Bean
    public NewTopic requestTopic() {
        return TopicBuilder
                .name(requestTopic)
                .build();
    }
}

package com.khaled.rbcassignment;

import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * This generator will always generate random int in the range of 0 < x < 100
 *
 * @author Khaled Mansour
 */

@Component
public class Generator4 extends Generator{
    private static final Logger log = LoggerFactory.getLogger(Generator4.class);

    @Setter private Map<Integer, RoundInputDto> lastRoundInputMap = new ConcurrentHashMap<>();

    @Override
    @KafkaListener(topics = {GameController.ROUND_START_TOPIC,GameController.ROUND_COMPLETED_TOPIC}, groupId = "Generator4")
    public void onMessage(ConsumerRecord<String, String> message) {
        super.onMessage(message);
    }

    @Override
    public int getGeneratorSeed(int gameControllerSeed) {
        return (int)Math.pow(gameControllerSeed,3 );
    }

    @Override
    public Map<Integer, RoundInputDto> getLastRoundInputMap() {
        return lastRoundInputMap;
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public String getName(){
        return this.getClass().getSimpleName();
    }
}

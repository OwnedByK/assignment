package com.khaled.rbcassignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * An object that will periodically generate a random int the range of 0 < x < 99
 *
 * @author Khaled Mansour
 */

@Component
public class SeedGenerator {
    private static final Logger log = LoggerFactory.getLogger(SeedGenerator.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Scheduled(fixedRateString = "${seedGenerator.interval}")
    public void generateSeed() {
        String seed = String.valueOf(ThreadLocalRandom.current().nextInt(1, 99));
        kafkaTemplate.send(GameController.ROOT_SEED_TOPIC,seed);
        log.trace("Root seed generated: {} ", seed);
    }
}

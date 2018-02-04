package com.khaled.rbcassignment;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListener;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * An object that represent a player who will always estimate
 * a random int in the range of 0 < x < 100
 *
 * @author Khaled Mansour
 */

public class Player implements MessageListener<String, String>{
    private static final Logger log = LoggerFactory.getLogger(Player.class);

    private String playerName;
    private KafkaTemplate kafkaTemplate;

    public Player(String playerName, KafkaTemplate kafkaTemplate ) {
        this.playerName  = playerName;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> message) {
        if(log.isTraceEnabled()){
            log.trace("{} received seed: {}", playerName , message.value());
        }

        RoundStartDto roundStartDto = new Gson().fromJson(message.value(), RoundStartDto.class);

        RoundInputDto roundInputDto = new RoundInputDto();
        roundInputDto.setValue(ThreadLocalRandom.current().nextInt(1, 100));
        roundInputDto.setSource(SourceEnum.PLAYER);
        roundInputDto.setSourceName(playerName);
        roundInputDto.setSimulationId(roundStartDto.getSimulationId());

        kafkaTemplate.send(GameController.ROUND_INPUT_TOPIC,new Gson().toJson(roundInputDto));
    }
}

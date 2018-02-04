package com.khaled.rbcassignment;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * An object that represent a prediction engine that will use
 * historical data to provide an estimate
 *
 * This object listens to the below event(s)
 *  ROUND_START_TOPIC
 *  ROUND_INPUT_AFTER_THE_FACT_TOPIC
 *
 * @author Khaled Mansour
 */

@Component
public class PredictionEngine {
    private static final Logger log = LoggerFactory.getLogger(PredictionEngine.class);

    List<Integer> historicalData = Collections.synchronizedList(new ArrayList<Integer>());

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @KafkaListener(topics = {GameController.ROUND_START_TOPIC,GameController.ROUND_INPUT_AFTER_THE_FACT_TOPIC})
    public void onMessage(ConsumerRecord<String, String> message) {
        if(log.isTraceEnabled()){
            log.trace("PredictionEngine received seed: {}", message.value());
        }

        if(GameController.ROUND_START_TOPIC.equals(message.topic())){
            listenOnRoundStart(message);
        } else if(GameController.ROUND_INPUT_AFTER_THE_FACT_TOPIC.equals(message.topic())){
            listenOnResultAfterTheFact(message);
        }
    }

    public void listenOnRoundStart(ConsumerRecord<String, String> message) {
        RoundStartDto roundStartDto = new Gson().fromJson(message.value(), RoundStartDto.class);

        RoundInputDto roundInputDto = new RoundInputDto();

        //If no historical data, pick random number
        if(historicalData.isEmpty()){
            log.debug("No History: picking random number");
            roundInputDto.setValue(ThreadLocalRandom.current().nextInt(1, 100));
        } else {
            //If there are historical data, pick random element from history
            Random rand = new Random();
            int randomElement = historicalData.get(rand.nextInt(historicalData.size()));
            log.debug("Picked: {} from Historical Data ",randomElement);
            roundInputDto.setValue(randomElement);
        }

        roundInputDto.setSource(SourceEnum.PREDICTION_ENGINE);
        roundInputDto.setSourceName("PredictionEngine");
        roundInputDto.setSimulationId(roundStartDto.getSimulationId());

        kafkaTemplate.send(GameController.ROUND_INPUT_TOPIC,new Gson().toJson(roundInputDto));
    }

    public void listenOnResultAfterTheFact(ConsumerRecord<String, String> message) {
        RoundInputDto roundInputDto = new Gson().fromJson(message.value(),RoundInputDto.class);
        log.debug("Received After The Fact from Source: {}  Value: {}", roundInputDto.getSourceName(), roundInputDto.getValue());
        historicalData.add(roundInputDto.getValue());
    }


}

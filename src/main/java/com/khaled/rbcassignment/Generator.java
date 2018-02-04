package com.khaled.rbcassignment;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.Random;

/**
 * This object is the parent for all Generators
 *
 * This object listens to the below event(s)
 *  ROUND_START_TOPIC
 *
 * @author Khaled Mansour
 */
public abstract class Generator {
    public abstract int getGeneratorSeed(int gameControllerSeed);
    public abstract Map<Integer, RoundInputDto> getLastRoundInputMap();
    public abstract Logger getLogger();
    public abstract String getName();

    @Autowired
    private KafkaTemplate kafkaTemplate;

    public void onMessage(ConsumerRecord<String, String> message) {
        if(getLogger().isTraceEnabled()){
            getLogger().trace("Received Seed: {}", message.value());
        }

        if(GameController.ROUND_START_TOPIC.equals(message.topic())){
            RoundStartDto roundStartDto = new Gson().fromJson(message.value(), RoundStartDto.class);

            RoundInputDto roundInputDto = new RoundInputDto();

            int generatorSeed = getGeneratorSeed(roundStartDto.getSeed());
            roundInputDto.setValue(new Random(generatorSeed).nextInt(100));

            roundInputDto.setSource(SourceEnum.GENERATOR);
            roundInputDto.setSourceName(getName());
            roundInputDto.setSimulationId(roundStartDto.getSimulationId());

            //Store the roundInput to be shared with the PredictionEngine once the round is completed
            getLastRoundInputMap().put(roundInputDto.getSimulationId(),roundInputDto);

            kafkaTemplate.send(GameController.ROUND_INPUT_TOPIC,new Gson().toJson(roundInputDto));
        } else if(GameController.ROUND_COMPLETED_TOPIC.equals(message.topic())){
            RoundResultDto roundResultDto = new Gson().fromJson(message.value(), RoundResultDto.class);
            RoundInputDto   roundInputDto = getLastRoundInputMap().get(roundResultDto.getSimulationId());
            kafkaTemplate.send( GameController.ROUND_INPUT_AFTER_THE_FACT_TOPIC ,new Gson().toJson(roundInputDto));
        }
    }
}

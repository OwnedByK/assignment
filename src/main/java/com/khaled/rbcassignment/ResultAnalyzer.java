package com.khaled.rbcassignment;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An object to receives inputs from Generators/Players/PredictionEngine
 * then calculate match points for each round
 *
 * This object listens to the below event(s)
 *  ROUND_START_TOPIC
 *  ROUND_INPUT_TOPIC
 *
 * @author Khaled Mansour
 */

@Component
public class ResultAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(ResultAnalyzer.class);

    @Getter @Setter private Map<Integer, RoundResultDto> roundsResultsMap = new ConcurrentHashMap<>();

    @Autowired
    @Getter @Setter private KafkaTemplate kafkaTemplate;


    @KafkaListener(topics = {GameController.ROUND_START_TOPIC, GameController.ROUND_INPUT_TOPIC}, groupId = "ResultAnalyzer")
    public void onMessage(ConsumerRecord<String, String> message) {
        if(GameController.ROUND_START_TOPIC.equals(message.topic())){
            listenOnRoundStart(message);
        } else if(GameController.ROUND_INPUT_TOPIC.equals(message.topic())){
            listenOnRoundInput(message);
        }
    }

    /**
     * Listen to Round Start event and prepare the data structure to
     * store simulation results
     * @param message
     */
    public void listenOnRoundStart(ConsumerRecord<String, String> message) {
        RoundStartDto roundStartDto = new Gson().fromJson(message.value(), RoundStartDto.class);
        RoundResultDto roundResultDto = roundsResultsMap.get(roundStartDto.getSimulationId());
        if(roundResultDto == null){
            roundResultDto = new RoundResultDto();
            roundResultDto.setSimulationId(roundStartDto.getSimulationId());
            roundResultDto.setNumberOfParticipatns(roundStartDto.getNumberOfPartictipants());
            roundsResultsMap.put(roundStartDto.getSimulationId(), roundResultDto);
        }

        //Set current round number and current season number
        roundResultDto.setRoundNumber(roundStartDto.getRoundNumber());
        roundResultDto.setSeasonNumber(roundStartDto.getSeasonNumber());
    }

    /**
     * Listen to Round Inputs from Generators, Players and Prediction Engine
     * then calculate the match points for the round once all inputs are received
     * @param message
     */
    public void listenOnRoundInput(ConsumerRecord<String, String> message) {
        //Receive current round inputs from Generators, Players and Prediction Engine
        RoundInputDto roundInputDto = new Gson().fromJson(message.value(), RoundInputDto.class);
        log.debug("Received: Simulation {}, Source: {}, Value: {}" , roundInputDto.getSimulationId(), roundInputDto.getSourceName() , roundInputDto.getValue());

        RoundResultDto roundResultDto = roundsResultsMap.get(roundInputDto.getSimulationId());

        if(roundInputDto.getSource().equals(SourceEnum.GENERATOR)){
            roundResultDto.getGeneratorsInputsForCurrentRound().add(roundInputDto);
        } else {
            roundResultDto.getPlayersInputsForCurrentRound().add(roundInputDto);
        }

        //Once all inputs are received, calculate match points then clear current round data and declare the round completed.
        int numberOfInputsExpected = roundResultDto.getNumberOfParticipatns() ;
        if(roundResultDto.getNumberOfInputsReceivedForCurrentRound().incrementAndGet() == numberOfInputsExpected){
            log.debug("All round inputs received.");
            Map playersResults = calculateMatchPoints(roundResultDto.getGeneratorsInputsForCurrentRound(), roundResultDto.getPlayersInputsForCurrentRound());

            if(log.isDebugEnabled()){
                log.debug("Players Results: {}", new Gson().toJson(playersResults));
            }

            roundResultDto.setResultForCurrentRound(playersResults);


            String roundResultJson = new Gson().toJson(roundResultDto);

            //Clear simulation current round data
            roundResultDto.getNumberOfInputsReceivedForCurrentRound().set(0);
            roundResultDto.getGeneratorsInputsForCurrentRound().clear();
            roundResultDto.getPlayersInputsForCurrentRound().clear();

            //Declare round completed
            kafkaTemplate.send(GameController.ROUND_COMPLETED_TOPIC,roundResultJson);
        }
    }

    /**
     * Calculate match points for all players
     * @param generatorsInput
     * @param playersInput
     * @return
     */
    public Map calculateMatchPoints(List<RoundInputDto> generatorsInput, List<RoundInputDto> playersInput) {
        Map<String,Integer> playersResults = new HashMap<>();
        for (RoundInputDto playerInput: playersInput) {
            int nofMatches = countMatches(playerInput.getValue(),generatorsInput);
            playersResults.put(playerInput.getSourceName(),nofMatches);
        }
        return playersResults;
    }

    /**
     * Calculate match point for single player
     * @param value
     * @param generatorsInput
     * @return
     */
    public int countMatches(int value, List<RoundInputDto> generatorsInput) {
        int counter = 0;
        for (RoundInputDto generatorInput: generatorsInput) {
            if(generatorInput.getValue()== value){
                counter++;
            }
        }
        return counter;
    }


}

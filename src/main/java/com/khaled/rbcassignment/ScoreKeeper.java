package com.khaled.rbcassignment;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This object is responsible for keeping the score.
 * It receives all rounds scores from ResultAnalyzer then count the score for each Player in the simulation
 *
 * This object is responsible for responding to the below API calls
 * GET /simulations
 * GET /simulations/{simulationId}
 *
 * This object listens to the below event(s)
 *  ROUND_COMPLETED_TOPIC
 *  SIMULATION_COMPLETED_TOPIC
 *
 *
 * @author Khaled Mansour
 */
@RestController
public class ScoreKeeper {
    private static final Logger log = LoggerFactory.getLogger(ScoreKeeper.class);

    @Getter @Setter private Map<Integer,SimulationScoreDto> allSimulationsScores     = new HashMap<>();
    @Getter @Setter private Multimap<Integer, RoundResultDto > allSimulationsDetailedResults = ArrayListMultimap.create();

    @RequestMapping(value = "/simulations", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Collection> getAllsimulations() {
        return new ResponseEntity<>(allSimulationsScores.values(),HttpStatus.OK);
    }

    @RequestMapping(value = "/simulations/{simulationId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Collection<RoundResultDto>> getSimulationById(@PathVariable("simulationId") int simulationId) {
        return new ResponseEntity<>(allSimulationsDetailedResults.get(simulationId),HttpStatus.OK);
    }

    @KafkaListener(topics = {GameController.ROUND_COMPLETED_TOPIC,GameController.SIMULATION_COMPLETED_TOPIC},groupId = "ScoreKeeper")
    public void onMessage(ConsumerRecord<String, String> message) {

        if(GameController.ROUND_COMPLETED_TOPIC.equals(message.topic())){
            listenOnRoundCompleted(message);
        } else if(GameController.SIMULATION_COMPLETED_TOPIC.equals(message.topic())){
            listenOnSimulationCompleted(message);
        }
    }

    public void listenOnSimulationCompleted(ConsumerRecord<String, String> message) {
        SimulationDto simulationDto = new Gson().fromJson(message.value(), SimulationDto.class);
        allSimulationsScores.get(simulationDto.getSimulationId()).setStatus(StatusEnum.COMPLETE);
    }

    public void listenOnRoundCompleted(ConsumerRecord<String, String> message) {
        RoundResultDto roundResultDto = new Gson().fromJson(message.value(), RoundResultDto.class);

        if(log.isDebugEnabled()){
            log.debug("Received Simulation {} > Season {} > Round {} : {}",roundResultDto.getSimulationId(),
                    roundResultDto.getSeasonNumber(),
                    roundResultDto.getRoundNumber(),
                    new Gson().toJson(roundResultDto.getResultForCurrentRound()) );
        }

        Map<String,AtomicInteger> playersScores;
        //If this is simulation first round, prepare data structure to store the results
        if(roundResultDto.getRoundNumber()==1 && roundResultDto.getSeasonNumber() == 1){

            SimulationScoreDto simulationScoreDto = new SimulationScoreDto();
            simulationScoreDto.setSimulationId(roundResultDto.getSimulationId());
            simulationScoreDto.setStatus(StatusEnum.RUNNING);
            playersScores = simulationScoreDto.getPlayersScore();

            for (String player : roundResultDto.getResultForCurrentRound().keySet()) {
                //Init score "0" for each player
                playersScores.put(player,new AtomicInteger(0));
            }
            allSimulationsScores.put(roundResultDto.getSimulationId(),simulationScoreDto);

        } else { //Data structure is already created in first round, find it and store it in local var
            playersScores = allSimulationsScores.get(roundResultDto.getSimulationId()).getPlayersScore();
        }

        for (String player : roundResultDto.getResultForCurrentRound().keySet()) {
            if(roundResultDto.getResultForCurrentRound().get(player) > 0){
                int playerRoundScore = roundResultDto.getResultForCurrentRound().get(player);
                playersScores.get(player).addAndGet(playerRoundScore);
            }
        }

        allSimulationsDetailedResults.put(roundResultDto.getSimulationId(),roundResultDto);

        if(log.isDebugEnabled()){
            log.debug("Current Score: {}", new Gson().toJson(playersScores));
        }
    }
}

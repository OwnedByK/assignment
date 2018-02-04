package com.khaled.rbcassignment;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * An object to run simulation run(s).  It controls the number of rounds/games per season,
 * number of seasons (per simulation), intervals between each round/game and each season.
 *
 * This object listens to the below event(s)
 *      ROOT_SEED_TOPIC         : generated from SeedGenerator and is being used as a seed for all Round Start seeds
 *      SIMULATION_START_TOPIC  : generated from the same object when it receives HTTP request to run a simulation
 *      ROUND_COMPLETED_TOPIC   : generated from ResultAnalyzer after receiving all inputs and calculating the result
 *
 * This object responds to the below API call(s)
 * POST /simulations
 *
 * @author Khaled Mansour
 */

@RestController
public class GameController implements MessageListener<String, String> {
    private static final Logger log = LoggerFactory.getLogger(GameController.class);

    private AtomicInteger simulationIdSequence = new AtomicInteger(0);

    private static final int    NUMBER_OF_GENERATORS_PLUS_PE     = 7;
    public static final String ROOT_SEED_TOPIC                  = "root.seed";
    public static final String SIMULATION_START_TOPIC           = "simulation.start";
    public static final String ROUND_START_TOPIC                = "round.start";
    public static final String ROUND_INPUT_TOPIC                = "round.input";
    public static final String ROUND_COMPLETED_TOPIC            = "round.completed";
    public static final String ROUND_INPUT_AFTER_THE_FACT_TOPIC = "round.input.afterTheFact";
    public static final String SIMULATION_COMPLETED_TOPIC       = "simulation.completed";


    @Getter @Setter private int latestSeed;
    @Getter @Setter private Map<Integer, SimulationDto> simulationsMap = new ConcurrentHashMap<>();
    @Getter @Setter private Random generator;


    @Autowired
    @Getter @Setter private KafkaTemplate kafkaTemplate;

    @Value("${numberOfPlayers}")
    private int numberOfPlayers;

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);

    @RequestMapping(value = "/simulations", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Void> simulations(@RequestBody SimulationDto simulationDto) {
        simulationDto.setSimulationId(simulationIdSequence.incrementAndGet());
        simulationsMap.put(simulationDto.getSimulationId(),simulationDto);

        kafkaTemplate.send(SIMULATION_START_TOPIC, new Gson().toJson(simulationDto));

        return new ResponseEntity<>(HttpStatus.OK);
    }

    @KafkaListener(topics = {ROOT_SEED_TOPIC, SIMULATION_START_TOPIC, ROUND_COMPLETED_TOPIC })
    public void onMessage(ConsumerRecord<String, String> message) {
        if (log.isTraceEnabled()) {
            log.trace("Received message: {}" ,message.value());
        }

        if(GameController.ROOT_SEED_TOPIC.equals(message.topic())){

            if(log.isTraceEnabled()){
                log.trace("Old Seed {} Expired - Received New Seed on [{}] : {}" , latestSeed , message.topic(), message.value());
            }
            latestSeed = Integer.parseInt(String.valueOf(message.value()));

        } else if(message.topic().contains(GameController.SIMULATION_START_TOPIC)){
            listenOnSimulationStart(message);
        } else if(message.topic().contains(GameController.ROUND_COMPLETED_TOPIC)){
            listenOnRoundCompleted(message);
        }
    }

    public void listenOnSimulationStart(ConsumerRecord<String, String> message) {
        if(log.isDebugEnabled()){
            log.debug("Received Starting Simulation on [{}] : {}" , message.topic(), message.value());
        }
        SimulationDto simulationDto = new Gson().fromJson(message.value(), SimulationDto.class);
        //Prepare the first round in the simulation
        generator = new Random(latestSeed);
        RoundStartDto roundStartDto = constructRoundStartDto(generator, simulationDto);

        simulationDto.getCurrentRoundNumber().incrementAndGet();

        kafkaTemplate.send(ROUND_START_TOPIC,new Gson().toJson(roundStartDto));
    }



    public void listenOnRoundCompleted(ConsumerRecord<String, String> message) {
        RoundResultDto roundResultDto = new Gson().fromJson(message.value(), RoundResultDto.class);
        SimulationDto simulationDto = simulationsMap.get(roundResultDto.getSimulationId());

        if(log.isDebugEnabled()){
            log.debug("Simulation {} > Season {} > Round {} completed: {} ", simulationDto.getSimulationId(),
                    simulationDto.getCurrentSeasonNumber(),simulationDto.getCurrentRoundNumber(),message.value());
        }
        //Check if season has more rounds, if yes ,then start next round
        if(simulationDto.getCurrentRoundNumber().get() < simulationDto.getNumberOfRounds()){

            int roundNumber = simulationDto.getCurrentRoundNumber().incrementAndGet();

            if(log.isDebugEnabled()){
                log.debug("Season has more rounds, Start Next Round: Simulation {} > Season {} > Round {}",
                        simulationDto.getSimulationId(), simulationDto.getCurrentSeasonNumber(), roundNumber);
            }

            RoundStartDto roundStartDto = constructRoundStartDto(generator, simulationDto);

            //Add interval between rounds
            executorService.schedule( () -> kafkaTemplate.send(ROUND_START_TOPIC,new Gson().toJson(roundStartDto))
                    , simulationDto.getIntervalBetweenRounds(), TimeUnit.MILLISECONDS);

        } else if (simulationDto.getCurrentSeasonNumber().get() < simulationDto.getNumberOfSeasons()){
            //^Check if simulation has more seasons, if yes , then start next season

            //reset round number and start next season
            simulationDto.getCurrentRoundNumber().set(1);
            int seasonNumber    = simulationDto.getCurrentSeasonNumber().incrementAndGet();
            log.debug("Season is completed, Start Next Season: Simulation {} > Season {} > Round {}",simulationDto.getSimulationId(), seasonNumber ,1);

            RoundStartDto roundStartDto = constructRoundStartDto(generator, simulationDto);

            //Add interval between seasons
            executorService.schedule( () -> kafkaTemplate.send(ROUND_START_TOPIC,new Gson().toJson(roundStartDto))
                    , simulationDto.getIntervalBetweenSeasons(), TimeUnit.MILLISECONDS);
        } else {
            log.debug("Simulation {} is completed", simulationDto.getSimulationId());
            simulationDto.setStatus(StatusEnum.COMPLETE);
            kafkaTemplate.send(SIMULATION_COMPLETED_TOPIC,new Gson().toJson(simulationDto));
        }
    }

    public RoundStartDto constructRoundStartDto(Random generator, SimulationDto simulationDto) {
        RoundStartDto roundStartDto = new RoundStartDto();
        roundStartDto.setSeed(generator.nextInt(100) );
        roundStartDto.setSimulationId(simulationDto.getSimulationId());
        roundStartDto.setNumberOfPartictipants( numberOfPlayers + GameController.NUMBER_OF_GENERATORS_PLUS_PE);

        roundStartDto.setRoundNumber(simulationDto.getCurrentRoundNumber().get());
        roundStartDto.setSeasonNumber(simulationDto.getCurrentSeasonNumber().get());
        return roundStartDto;
    }


}

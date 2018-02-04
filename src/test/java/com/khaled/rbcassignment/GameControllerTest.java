package com.khaled.rbcassignment;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class GameControllerTest {

    @Mock
    private KafkaTemplate kafkaTemplate;

    GameController gameController;

    @Before
    public void setup(){
        gameController = new GameController();
        gameController.setKafkaTemplate(kafkaTemplate);
    }

    @Test
    public void rootSeedStoredAsLatest() throws Exception {


        ConsumerRecord consumerRecord1 = new ConsumerRecord<String, String>(GameController.ROOT_SEED_TOPIC,
                0, 0L, "mykey", "5" );
        gameController.onMessage(consumerRecord1);

        assertThat(gameController.getLatestSeed()).isEqualTo(5);

        ConsumerRecord consumerRecord2 = new ConsumerRecord<String, String>(GameController.ROOT_SEED_TOPIC,
                0, 0L, "mykey", "6" );
        gameController.onMessage(consumerRecord2);

        assertThat(gameController.getLatestSeed()).isEqualTo(6);

    }

    @Test
    public void seasonFirstRoundIsCompleted(){
        SimulationDto simulationDto = new SimulationDto();
        simulationDto.setSimulationId(1);
        simulationDto.setNumberOfRounds(3);
        simulationDto.setNumberOfSeasons(2);

        gameController.getSimulationsMap().put(simulationDto.getSimulationId(),simulationDto);
        gameController.setGenerator(new Random());

        ConsumerRecord consumerRecord = new ConsumerRecord<String, String>(GameController.ROOT_SEED_TOPIC,
                0, 0L, "mykey", new Gson().toJson(simulationDto ));

        gameController.listenOnRoundCompleted(consumerRecord);

        //Check that next rounded is started
        assertThat(simulationDto.getCurrentRoundNumber().get()).isEqualTo(2);
        assertThat(simulationDto.getCurrentSeasonNumber().get()).isEqualTo(1);
    }


    @Test
    public void firstSeasonLastRoundIsCompleted(){

        SimulationDto simulationDto = new SimulationDto();
        simulationDto.setSimulationId(1);
        simulationDto.setNumberOfRounds(3);
        simulationDto.setNumberOfSeasons(2);
        simulationDto.setCurrentRoundNumber(new AtomicInteger(3));
        simulationDto.setCurrentSeasonNumber(new AtomicInteger(1));

        gameController.getSimulationsMap().put(simulationDto.getSimulationId(),simulationDto);
        gameController.setGenerator(new Random());

        ConsumerRecord consumerRecord = new ConsumerRecord<String, String>(GameController.ROOT_SEED_TOPIC,
                0, 0L, "mykey", new Gson().toJson(simulationDto ));

        gameController.listenOnRoundCompleted(consumerRecord);

        //Check that next season first round is started
        assertThat(simulationDto.getCurrentRoundNumber().get()).isEqualTo(1);
        assertThat(simulationDto.getCurrentSeasonNumber().get()).isEqualTo(2);
    }

    @Test
    public void lastSeasonLastRoundIsCompleted(){

        SimulationDto simulationDto = new SimulationDto();
        simulationDto.setSimulationId(1);
        simulationDto.setNumberOfRounds(3);
        simulationDto.setNumberOfSeasons(2);
        simulationDto.setCurrentRoundNumber(new AtomicInteger(3));
        simulationDto.setCurrentSeasonNumber(new AtomicInteger(2));

        gameController.getSimulationsMap().put(simulationDto.getSimulationId(),simulationDto);
        gameController.setGenerator(new Random());

        ConsumerRecord consumerRecord = new ConsumerRecord<String, String>(GameController.ROOT_SEED_TOPIC,
                0, 0L, "mykey", new Gson().toJson(simulationDto ));

        gameController.listenOnRoundCompleted(consumerRecord);

        //Check that the simulation is marked as complete
        assertThat(simulationDto.getStatus()).isEqualTo(StatusEnum.COMPLETE);
    }

}

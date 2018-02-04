package com.khaled.rbcassignment;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import static org.junit.Assert.*;
import java.util.ArrayList;
import static org.assertj.core.api.Assertions.assertThat;


@RunWith(MockitoJUnitRunner.class)
public class ResultAnalyzerTest {

    @Mock
    private KafkaTemplate kafkaTemplate;

    private ResultAnalyzer resultAnalyzer;

    @Before
    public void setup(){
        resultAnalyzer = new ResultAnalyzer();
        resultAnalyzer.setKafkaTemplate(kafkaTemplate);
    }

    @Test
    public void roundStartedAndResultsPartiallyReceived(){
        int SIMULATION_ID = 1;

        RoundResultDto simulationResultsDto = new RoundResultDto();
        simulationResultsDto.setNumberOfParticipatns(10);
        simulationResultsDto.setSimulationId(SIMULATION_ID);

        resultAnalyzer.getRoundsResultsMap().put(SIMULATION_ID,simulationResultsDto);

        ArrayList<RoundInputDto> list = new ArrayList();
        RoundInputDto roundInput1 = new RoundInputDto();
        roundInput1.setSimulationId(SIMULATION_ID);
        roundInput1.setSource(SourceEnum.GENERATOR);
        roundInput1.setSourceName("Generator1");
        roundInput1.setValue(1);
        list.add(roundInput1);

        RoundInputDto roundInput2 = new RoundInputDto();
        roundInput2.setSimulationId(SIMULATION_ID);
        roundInput2.setSource(SourceEnum.GENERATOR);
        roundInput2.setSourceName("Generator2");
        roundInput2.setValue(2);
        list.add(roundInput2);

        RoundInputDto roundInput3 = new RoundInputDto();
        roundInput3.setSimulationId(SIMULATION_ID);
        roundInput3.setSource(SourceEnum.GENERATOR);
        roundInput3.setSourceName("Generator3");
        roundInput3.setValue(3);
        list.add(roundInput3);

        RoundInputDto roundInput4 = new RoundInputDto();
        roundInput4.setSimulationId(SIMULATION_ID);
        roundInput4.setSource(SourceEnum.GENERATOR);
        roundInput4.setSourceName("Generator4");
        roundInput4.setValue(4);
        list.add(roundInput4);

        RoundInputDto roundInput5 = new RoundInputDto();
        roundInput5.setSimulationId(SIMULATION_ID);
        roundInput5.setSource(SourceEnum.GENERATOR);
        roundInput5.setSourceName("Generator5");
        roundInput5.setValue(5);
        list.add(roundInput5);

        RoundInputDto roundInput6 = new RoundInputDto();
        roundInput6.setSimulationId(SIMULATION_ID);
        roundInput6.setSource(SourceEnum.GENERATOR);
        roundInput6.setSourceName("Generator6");
        roundInput6.setValue(6);
        list.add(roundInput6);

        RoundInputDto player1 = new RoundInputDto();
        player1.setSimulationId(SIMULATION_ID);
        player1.setSource(SourceEnum.PLAYER);
        player1.setSourceName("Player1");
        player1.setValue(1);
        list.add(player1);


        for (RoundInputDto roundInput: list) {
            ConsumerRecord consumerRecord = new ConsumerRecord<String, String>(GameController.ROUND_INPUT_TOPIC,
                    0, 0L, "mykey", new Gson().toJson(roundInput) );
            resultAnalyzer.onMessage(consumerRecord);
        }

        //Since not all inputs are received (only 7 out of 10), check that the counter is not resetted and is equal to the number of test inputs .
        RoundResultDto roundResultDto = resultAnalyzer.getRoundsResultsMap().get(SIMULATION_ID);
        assertThat(roundResultDto.getNumberOfInputsReceivedForCurrentRound().get()).isEqualTo(7);
    }


    @Test
    public void roundStartedAndResultsFullyReceived(){
        int SIMULATION_ID = 1;

        RoundResultDto simulationResultsDto = new RoundResultDto();
        simulationResultsDto.setNumberOfParticipatns(10);
        simulationResultsDto.setSimulationId(SIMULATION_ID);

        resultAnalyzer.getRoundsResultsMap().put(SIMULATION_ID,simulationResultsDto);


        ArrayList<RoundInputDto> list = new ArrayList();
        RoundInputDto roundInput1 = new RoundInputDto();
        roundInput1.setSimulationId(SIMULATION_ID);
        roundInput1.setSource(SourceEnum.GENERATOR);
        roundInput1.setSourceName("Generator1");
        roundInput1.setValue(1);
        list.add(roundInput1);

        RoundInputDto roundInput2 = new RoundInputDto();
        roundInput2.setSimulationId(SIMULATION_ID);
        roundInput2.setSource(SourceEnum.GENERATOR);
        roundInput2.setSourceName("Generator2");
        roundInput2.setValue(2);
        list.add(roundInput2);

        RoundInputDto roundInput3 = new RoundInputDto();
        roundInput3.setSimulationId(SIMULATION_ID);
        roundInput3.setSource(SourceEnum.GENERATOR);
        roundInput3.setSourceName("Generator3");
        roundInput3.setValue(2);
        list.add(roundInput3);

        RoundInputDto roundInput4 = new RoundInputDto();
        roundInput4.setSimulationId(SIMULATION_ID);
        roundInput4.setSource(SourceEnum.GENERATOR);
        roundInput4.setSourceName("Generator4");
        roundInput4.setValue(4);
        list.add(roundInput4);

        RoundInputDto roundInput5 = new RoundInputDto();
        roundInput5.setSimulationId(SIMULATION_ID);
        roundInput5.setSource(SourceEnum.GENERATOR);
        roundInput5.setSourceName("Generator5");
        roundInput5.setValue(5);
        list.add(roundInput5);

        RoundInputDto roundInput6 = new RoundInputDto();
        roundInput6.setSimulationId(SIMULATION_ID);
        roundInput6.setSource(SourceEnum.GENERATOR);
        roundInput6.setSourceName("Generator6");
        roundInput6.setValue(6);
        list.add(roundInput6);

        RoundInputDto player1 = new RoundInputDto();
        player1.setSimulationId(SIMULATION_ID);
        player1.setSource(SourceEnum.PLAYER);
        player1.setSourceName("Player1");
        player1.setValue(1);
        list.add(player1);

        RoundInputDto player2 = new RoundInputDto();
        player2.setSimulationId(SIMULATION_ID);
        player2.setSource(SourceEnum.PLAYER);
        player2.setSourceName("Player2");
        player2.setValue(2);
        list.add(player2);

        RoundInputDto player3 = new RoundInputDto();
        player3.setSimulationId(SIMULATION_ID);
        player3.setSource(SourceEnum.PLAYER);
        player3.setSourceName("Player3");
        player3.setValue(3);
        list.add(player3);

        RoundInputDto pe = new RoundInputDto();
        pe.setSimulationId(SIMULATION_ID);
        pe.setSource(SourceEnum.PLAYER);
        pe.setSourceName("PE");
        pe.setValue(1);
        list.add(pe);


        for (RoundInputDto roundInput: list) {
            ConsumerRecord consumerRecord = new ConsumerRecord<String, String>(GameController.ROUND_INPUT_TOPIC,
                    0, 0L, "mykey", new Gson().toJson(roundInput) );
            resultAnalyzer.onMessage(consumerRecord);
        }



        //Verify that the counter is resetted once all round inputs are received
        RoundResultDto roundResultDto = resultAnalyzer.getRoundsResultsMap().get(SIMULATION_ID);
        assertEquals(0, roundResultDto.getNumberOfInputsReceivedForCurrentRound().get());

        //Check players scores
        assertEquals(1, roundResultDto.getResultForCurrentRound().get("Player1").intValue());
        assertEquals(2, roundResultDto.getResultForCurrentRound().get("Player2").intValue());
        assertEquals(1, roundResultDto.getResultForCurrentRound().get("PE").intValue());
    }


}

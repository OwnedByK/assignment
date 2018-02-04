package com.khaled.rbcassignment;

import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class RoundResultDto {
    private int simulationId;
    private int roundNumber;
    private int seasonNumber;

    private List<RoundInputDto> generatorsInputsForCurrentRound = Collections.synchronizedList(new ArrayList<>());
    private List<RoundInputDto> playersInputsForCurrentRound = Collections.synchronizedList(new ArrayList<>());
    private Map<String,Integer> resultForCurrentRound = new ConcurrentHashMap<>();
    private AtomicInteger numberOfInputsReceivedForCurrentRound = new AtomicInteger(0);
    private int numberOfParticipatns;
}

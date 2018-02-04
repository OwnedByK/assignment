package com.khaled.rbcassignment;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class SimulationScoreDto {
    private int simulationId;
    private StatusEnum status;
    private Map<String, AtomicInteger> playersScore = new HashMap<>();

}

package com.khaled.rbcassignment;

import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;

@Data
public class SimulationDto {
    private int simulationId;
    private int numberOfSeasons;
    private int intervalBetweenSeasons;
    private int numberOfRounds;
    private int intervalBetweenRounds;

    private AtomicInteger currentRoundNumber = new AtomicInteger(1);
    private AtomicInteger currentSeasonNumber = new AtomicInteger(1);
    private StatusEnum status = StatusEnum.RUNNING;
}

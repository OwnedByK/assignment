package com.khaled.rbcassignment;

import lombok.Data;

@Data
public class RoundStartDto {
    private int simulationId;
    private int seed;
    private int numberOfPartictipants;

    private int roundNumber;
    private int seasonNumber;
}

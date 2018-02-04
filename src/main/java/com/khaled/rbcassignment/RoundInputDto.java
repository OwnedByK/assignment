package com.khaled.rbcassignment;

import lombok.Data;

import java.io.Serializable;

@Data
public class RoundInputDto implements Serializable{
    private int simulationId;
    private SourceEnum source;
    private String sourceName;
    private int value;
}

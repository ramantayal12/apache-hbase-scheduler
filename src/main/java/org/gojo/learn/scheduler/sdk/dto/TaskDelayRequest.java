package org.gojo.learn.scheduler.sdk.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskDelayRequest {

    private String type;
    private long delayInSeconds;
    private String payload;

}

package com.colak.springtutorial.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@Document(collection="trips")
public class Trips {

    @Id
    private String id;

    @Field("tripduration")
    private Integer duration;

    @Field("start station id")
    private Integer startStationId;

    @Field("start station name")
    private String startStationName;

    @Field("end station id")
    private Integer endStationId;

    @Field("end station name")
    private String endStationName;

    @Field("bikeid")
    private Integer bikeId;

    @Field("usertype")
    private String userType;

    @Field("birth year")
    private Integer birthYear;

    private Integer gender;

    @Field("start station location")
    private StationLocation startStationLocation;

    @Field("end station location")
    private StationLocation endStationLocation;

    @Field("start time")
    private LocalDateTime startTime;

    @Field("stop time")
    private LocalDateTime stopTime;
}

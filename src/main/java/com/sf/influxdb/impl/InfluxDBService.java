package com.sf.influxdb.impl;

import com.sf.influxdb.dto.Results;
import com.sf.influxdb.dto.Write;
import retrofit.http.Body;
import retrofit.http.GET;
import retrofit.http.POST;
import retrofit.http.Query;

interface InfluxDBService {
  public static final String U = "u";
  public static final String P = "p";
  public static final String Q = "q";
  public static final String DB = "db";

  @POST("/write")
  public String write(@Query(U) String username, @Query(P) String password, @Body Write write);

  @GET("/query")
  public Results query(
      @Query(U) String username,
      @Query(P) String password,
      @Query(DB) String database,
      @Query(Q) String command
  );
}
